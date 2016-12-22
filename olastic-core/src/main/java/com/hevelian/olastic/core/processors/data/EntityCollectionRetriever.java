package com.hevelian.olastic.core.processors.data;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import com.hevelian.olastic.core.api.uri.queryoption.expression.member.impl.ExpressionResult;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.SearchOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.search.SearchBinary;
import org.apache.olingo.server.api.uri.queryoption.search.SearchBinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.search.SearchExpression;
import org.apache.olingo.server.api.uri.queryoption.search.SearchUnary;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.ElasticConstants;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.utils.ApplyOptionUtils;
import com.hevelian.olastic.core.utils.ProcessorUtils;

import lombok.extern.log4j.Log4j2;

/**
 * Provides high-level methods for retrieving and converting the data for
 * collection of entities.
 * 
 * @author rdidyk
 */
@Log4j2
public class EntityCollectionRetriever extends DataRetriever {

    /**
     * Fully initializes {@link EntityCollectionRetriever}.
     *
     * @param uriInfo
     *            uriInfo object
     * @param odata
     *            odata instance
     * @param client
     *            ES raw client
     * @param rawBaseUri
     *            war base uri
     * @param serviceMetadata
     *            service metadata
     * @param responseFormat
     *            response format
     */
    public EntityCollectionRetriever(UriInfo uriInfo, ElasticOData odata, Client client,
            String rawBaseUri, ElasticServiceMetadata serviceMetadata, ContentType responseFormat) {
        super(uriInfo, odata, client, rawBaseUri, serviceMetadata, responseFormat);
    }

    @Override
    public SerializerResult getSerializedData() throws ODataApplicationException {
        QueryWithEntity queryWithEntity = getQueryWithEntity();
        ElasticEdmEntitySet entitySet = queryWithEntity.getEntitySet();
        ESQueryBuilder queryBuilder = queryWithEntity.getQuery();

        return serialize(retrieveData(queryBuilder), entitySet);
    }

    /**
     * Method get's filter query from URL.
     * 
     * @return filter query
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected QueryBuilder getFilterQuery() throws ODataApplicationException {
        FilterOption filterOption = getUriInfo().getFilterOption();
        ApplyOption applyOption = getUriInfo().getApplyOption();
        BoolQueryBuilder filterQuery = new BoolQueryBuilder();
        try {
            if (filterOption != null) {
                Expression expression = filterOption.getExpression();
                QueryBuilder query = ((ExpressionResult) expression.accept(
                        new ElasticSearchExpressionVisitor())).getQueryBuilder();
                filterQuery.filter(query);
            } else if (applyOption != null) {
                List<Expression> expressions = ApplyOptionUtils.getFilters(applyOption).stream()
                        .map(e -> e.getFilterOption().getExpression()).collect(Collectors.toList());
                for (Expression expression : expressions) {
                    filterQuery.filter(
                            (QueryBuilder) expression.accept(new ElasticSearchExpressionVisitor()));
                }
            }
        } catch (ExpressionVisitException e) {
            log.debug(e);
        }
        return filterQuery;
    }

    /**
     * Method creates search query from $search system query option from URL.
     * 
     * @return search query
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected QueryBuilder getSearchQuery() throws ODataApplicationException {
        SearchOption searchOption = getUriInfo().getSearchOption();
        ApplyOption applyOption = getUriInfo().getApplyOption();
        BoolQueryBuilder searchQuery = new BoolQueryBuilder();
        if (searchOption != null) {
            buildSearchQuery(searchQuery, searchOption.getSearchExpression());
        } else if (applyOption != null) {
            List<SearchExpression> expressions = ApplyOptionUtils.getSearchItems(applyOption)
                    .stream().map(e -> e.getSearchOption().getSearchExpression())
                    .collect(Collectors.toList());
            for (SearchExpression expression : expressions) {
                buildSearchQuery(searchQuery, expression);
            }
        }
        return searchQuery;
    }

    /**
     * Method create's query for $search expression.
     * 
     * @param parentQuery
     *            parent bool query
     * @param expression
     *            search expression from query option
     */
    private void buildSearchQuery(BoolQueryBuilder parentQuery, SearchExpression expression) {
        if (expression.isSearchBinary()) {
            SearchBinary binary = expression.asSearchBinary();
            BoolQueryBuilder leftBool = QueryBuilders.boolQuery();
            BoolQueryBuilder rightBool = QueryBuilders.boolQuery();
            SearchBinaryOperatorKind operatorKind = binary.getOperator();
            if (operatorKind == SearchBinaryOperatorKind.AND) {
                parentQuery.must(leftBool);
                parentQuery.must(rightBool);
            } else if (operatorKind == SearchBinaryOperatorKind.OR) {
                parentQuery.should(leftBool);
                parentQuery.should(rightBool);
            }
            buildSearchQuery(leftBool, binary.getLeftOperand());
            buildSearchQuery(rightBool, binary.getRightOperand());
        } else if (expression.isSearchUnary()) {
            SearchUnary unary = expression.asSearchUnary();
            parentQuery.mustNot(
                    matchQuery(ElasticConstants.ALL_FIELD, unary.getOperand().getSearchTerm()));
        } else {
            parentQuery.must(matchQuery(ElasticConstants.ALL_FIELD,
                    expression.asSearchTerm().getSearchTerm()));
        }
    }

    @Override
    protected SearchResponse retrieveData(ESQueryBuilder query) throws ODataApplicationException {
        return ESClient.executeRequest(query.getIndex(), query.getType(),
                getClient(), new BoolQueryBuilder().filter(query.getQuery())
                        .filter(getFilterQuery()).filter(getSearchQuery()),
                getPagination(), query.getFields());
    }

    @Override
    protected SerializerResult serialize(SearchResponse response, ElasticEdmEntitySet entitySet)
            throws ODataApplicationException {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        EntityCollection entities = new EntityCollection();
        for (SearchHit hit : response.getHits()) {
            Entity entity = new Entity();
            entity.setId(ProcessorUtils.createId(entityType.getName(), hit.getId()));
            addProperty(entity, ElasticConstants.ID_FIELD_NAME, hit.getId(), entityType);

            for (Map.Entry<String, Object> entry : hit.getSource().entrySet()) {
                addProperty(entity, entityType.findPropertyByEField(entry.getKey()).getName(),
                        entry.getValue(), entityType);
            }
            entities.getEntities().add(entity);
        }
        if (isCount()) {
            entities.setCount((int) response.getHits().getTotalHits());
        }
        return serializeEntities(entities, entitySet);
    }

    /**
     * Serializes entities.
     *
     * @param entities
     *            entities
     * @param entitySet
     *            entitySet
     * @return serialized data
     * @throws ODataApplicationException
     *             if any error occurred during serialization
     */
    protected SerializerResult serializeEntities(EntityCollection entities,
            ElasticEdmEntitySet entitySet) throws ODataApplicationException {
        ElasticEdmEntityType entityType = entitySet.getEntityType();
        String id = getRawBaseUri() + "/" + entityType.getName();
        ExpandOption expand = getUriInfo().getExpandOption();
        SelectOption select = getUriInfo().getSelectOption();
        try {
            ODataSerializer serializer = getOdata().createSerializer(getResponseFormat());
            return serializer
                    .entityCollection(getServiceMetadata(), entityType, entities,
                            EntityCollectionSerializerOptions.with()
                                    .contextURL(
                                            getContextUrl(entitySet, false, expand, select, null))
                                    .id(id).count(getUriInfo().getCountOption()).select(select)
                                    .expand(expand).build());
        } catch (SerializerException e) {
            throw new ODataApplicationException("Failed to serialize data.",
                    HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ROOT, e);
        }
    }

}
