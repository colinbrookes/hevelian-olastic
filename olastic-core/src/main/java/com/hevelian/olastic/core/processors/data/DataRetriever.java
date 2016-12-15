package com.hevelian.olastic.core.processors.data;

import java.util.*;

import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.ContextURL.Suffix;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePartTyped;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.core.uri.UriResourceNavigationPropertyImpl;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.api.uri.queryoption.expression.ElasticSearchExpressionVisitor;
import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.ESClient;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.pagination.Pagination;
import com.hevelian.olastic.core.elastic.pagination.Sort;
import com.hevelian.olastic.core.utils.ProcessorUtils;

import lombok.extern.log4j.Log4j2;

import javax.xml.bind.DatatypeConverter;

/**
 * This class provides high-level methods for retrieving and converting the
 * data. It contains all the metadata and request parameters needed for
 * requesting and serializing the data.
 */
@Log4j2
public abstract class DataRetriever {

    private UriInfo uriInfo;
    private ElasticOData odata;
    private Client client;
    private String rawBaseUri;
    private ElasticServiceMetadata serviceMetadata;
    private ContentType responseFormat;

    /**
     * Fully initializes {@link DataRetriever}.
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
    public DataRetriever(UriInfo uriInfo, ElasticOData odata, Client client, String rawBaseUri,
            ElasticServiceMetadata serviceMetadata, ContentType responseFormat) {
        this.uriInfo = uriInfo;
        this.odata = odata;
        this.client = client;
        this.rawBaseUri = rawBaseUri;
        this.serviceMetadata = serviceMetadata;
        this.responseFormat = responseFormat;
    }

    /**
     * Retrieves and serializes the data.
     *
     * @return serialized data
     * @throws ODataApplicationException
     *             if any error occurred during getting serialized data
     */
    public SerializerResult getSerializedData() throws ODataApplicationException {
        QueryWithEntity queryWithEntity = getQueryWithEntity();
        ElasticEdmEntitySet entitySet = queryWithEntity.getEntitySet();
        ESQueryBuilder queryBuilder = queryWithEntity.getQuery();
        SearchResponse searchResponse = retrieveData(queryBuilder, getFilterQuery());

        return serialize(searchResponse, entitySet);
    }

    /**
     * Method get's filter query from URL.
     * 
     * @return filter query
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected QueryBuilder getFilterQuery() throws ODataApplicationException {
        FilterOption filterOption = uriInfo.getFilterOption();
        QueryBuilder filterQueryBuilder = null;
        if (filterOption != null) {
            Expression expression = filterOption.getExpression();
            try {
                filterQueryBuilder = (QueryBuilder) expression
                        .accept(new ElasticSearchExpressionVisitor());
            } catch (ExpressionVisitException e) {
                log.debug(e);
            }
        }
        if (filterQueryBuilder == null) {
            filterQueryBuilder = new MatchAllQueryBuilder();
        }
        return filterQueryBuilder;
    }

    /**
     * Serializes response from ES.
     *
     * @param response
     *            ES response
     * @param entitySet
     *            entitySet
     * @return serialized data
     * @throws ODataApplicationException
     *             if any error occurred during serialization
     */
    protected abstract SerializerResult serialize(SearchResponse response,
            ElasticEdmEntitySet entitySet) throws ODataApplicationException;

    /**
     * Creates context URL for response serializer.
     * 
     * @param edmEntitySet
     *            entity set
     * @param isSingleEntity
     *            is single entity
     * @param expand
     *            expand option
     * @param select
     *            select option
     * @param navOrPropertyPath
     *            property path
     * @return created context URL
     * @throws SerializerException
     *             if any error occurred
     */
    protected ContextURL getContextUrl(ElasticEdmEntitySet entitySet, boolean isSingleEntity,
            ExpandOption expand, SelectOption select, String navOrPropertyPath)
            throws SerializerException {
        return ContextURL.with().entitySet(entitySet)
                .selectList(odata.createUriHelper()
                        .buildContextURLSelectList(entitySet.getEntityType(), expand, select))
                .suffix(isSingleEntity ? Suffix.ENTITY : null).navOrPropertyPath(navOrPropertyPath)
                .build();
    }

    /**
     * Returns the list of fields from URL.
     *
     * @return fields fields from URL
     */
    protected List<String> getSelectList() {
        List<String> result = new ArrayList<>();
        SelectOption selectOption = uriInfo.getSelectOption();
        if (selectOption != null) {
            List<SelectItem> selectItems = selectOption.getSelectItems();
            for (SelectItem selectItem : selectItems) {
                List<UriResource> selectParts = selectItem.getResourcePath().getUriResourceParts();
                String fieldName = selectParts.get(selectParts.size() - 1).getSegmentValue();
                result.add(fieldName);
            }
        }
        return result;
    }

    /**
     * Builds the query builder for requesting the data and entity set for
     * serializing.
     *
     * @return Query builder and entity set
     * @throws ODataApplicationException
     */
    protected QueryWithEntity getQueryWithEntity() throws ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResourceEntitySet firstUriResourceEntitySet = getFirstResourceEntitySet();

        ElasticEdmEntitySet responseEntitySet = (ElasticEdmEntitySet) firstUriResourceEntitySet
                .getEntitySet();
        ESQueryBuilder queryBuilder = new ESQueryBuilder();
        for (int i = 0; i < getUsefulPartsSize(); i++) {
            UriResource segment = resourceParts.get(i);
            if (segment.getKind() == UriResourceKind.primitiveProperty) {
                break;
            }
            if (segment.getKind() == UriResourceKind.navigationProperty) {
                UriResourceNavigation uriResourceNavigation = (UriResourceNavigation) segment;
                EdmNavigationProperty navigationProperty = uriResourceNavigation.getProperty();
                responseEntitySet = ProcessorUtils.getNavigationTargetEntitySet(responseEntitySet,
                        navigationProperty);
            } else if (segment.getKind() != UriResourceKind.entitySet) {
                throw new ODataApplicationException("Not supported",
                        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
            }
            buildQuery(queryBuilder, i);
        }
        ElasticEdmEntityType entityType = responseEntitySet.getEntityType();
        queryBuilder.setType(entityType.getEType()).setIndex(entityType.getEIndex());
        for (String fieldName : getSelectList()) {
            queryBuilder.addField(entityType.getEProperties().get(fieldName).getEField());
        }
        return new QueryWithEntity(responseEntitySet, queryBuilder);
    }

    /**
     * Builds query to elasticsearch using given part of the url.
     *
     * @param query
     *            query builder that should be updated with a query for given
     *            part of the url
     * @param urlPartIndex
     *            index of the part of url for which query should be added
     * @throws ODataApplicationException
     */
    private void buildQuery(ESQueryBuilder query, int urlPartIndex)
            throws ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResource segment = resourceParts.get(urlPartIndex);
        boolean isLast = urlPartIndex == getUsefulPartsSize() - 1;
        String type = ((UriResourcePartTyped) segment).getType().getName();
        List<String> ids = collectIds(segment);
        if (!isLast) {
            UriResource nextSegment = resourceParts.get(urlPartIndex + 1);
            if (((UriResourceNavigationPropertyImpl) nextSegment).getProperty().isCollection()) {
                query.addParentQuery(type, ids);
            } else {
                query.addChildQuery(type, ids);
            }
        } else {
            query.addIdsQuery(type, ids);
        }
    }

    /**
     * Returns the size of the url parts that are involved in the query
     * building.
     *
     * @return useful url parts size
     */
    protected int getUsefulPartsSize() {
        return uriInfo.getUriResourceParts().size();
    }

    /**
     * Retrieves ids from uri resource part.
     *
     * @param segment
     *            uri resource part
     * @return ids list
     */
    protected List<String> collectIds(UriResource segment) throws ODataApplicationException {
        List<String> ids = new ArrayList<>();
        List<UriParameter> keyPredicates;
        if (segment instanceof UriResourceNavigation) {
            keyPredicates = ((UriResourceNavigation) segment).getKeyPredicates();
        } else {
            keyPredicates = ((UriResourceEntitySet) segment).getKeyPredicates();
        }
        if (keyPredicates.size() > 1) {
            throw new ODataApplicationException("Composite Keys are not supported",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
        }
        for (UriParameter param : keyPredicates) {
            ids.add(param.getText().replaceAll("\'", ""));
        }
        return ids;
    }

    /**
     * Gets the data from ES.
     *
     * @param query
     *            query builder
     * @param filter
     *            raw ES query with filter
     * @return ES response
     * @throws ODataApplicationException
     */
    protected SearchResponse retrieveData(ESQueryBuilder query, QueryBuilder filter)
            throws ODataApplicationException {
        return ESClient.executeRequest(query.getIndex(), query.getType(), client,
                new BoolQueryBuilder().filter(query.getQuery()).filter(filter), getPagination(),
                query.getFields());
    }

    /**
     * Checks if URI has count option.
     *
     * @return true if there is count option in the url
     */
    protected boolean isCount() {
        // handle $count: always return the original number of entities, without
        // considering $top and $skip
        boolean isCount = false;
        CountOption countOption = uriInfo.getCountOption();
        if (countOption != null) {
            isCount = countOption.getValue();
        }
        return isCount;
    }

    protected UriResourceEntitySet getFirstResourceEntitySet() throws ODataApplicationException {
        List<UriResource> resourceParts = uriInfo.getUriResourceParts();
        UriResource uriResource = resourceParts.get(0);
        if (!(uriResource instanceof UriResourceEntitySet)) {
            throw new ODataApplicationException("Only EntitySet is supported",
                    HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
        }
        return (UriResourceEntitySet) uriResource;
    }

    /**
     * Returns pagination data.
     *
     * @return pagination
     */
    protected Pagination getPagination() {
        int skipNumber = Pagination.SKIP_DEFAULT;
        SkipOption skipOption = uriInfo.getSkipOption();
        if (skipOption != null) {
            skipNumber = skipOption.getValue();
        }

        int topNumber = Pagination.TOP_DEFAULT;
        TopOption topOption = uriInfo.getTopOption();
        if (topOption != null) {
            topNumber = topOption.getValue();
        }

        OrderByOption orderByOption = uriInfo.getOrderByOption();
        List<Sort> orderBy = new ArrayList<>();
        if (orderByOption != null) {
            List<OrderByItem> orderItemList = orderByOption.getOrders();
            for (OrderByItem orderByItem : orderItemList) {
                Expression expression = orderByItem.getExpression();
                if (expression instanceof Member) {
                    UriInfoResource resourcePath = ((Member) expression).getResourcePath();
                    UriResource oUriResource = resourcePath.getUriResourceParts().get(0);
                    if (oUriResource instanceof UriResourcePrimitiveProperty) {
                        EdmProperty edmProperty = ((UriResourcePrimitiveProperty) oUriResource)
                                .getProperty();
                        orderBy.add(new Sort(edmProperty.getName(), orderByItem.isDescending()
                                ? Sort.Direction.DESC : Sort.Direction.ASC));
                    }
                }
            }
        }
        return new Pagination(topNumber, skipNumber, orderBy);
    }

    @SuppressWarnings("unchecked")
    protected void addProperty(Entity e, String name, Object value,
            ElasticEdmEntityType entityType) {
        if (value instanceof List) {
            e.addProperty(createPropertyList(name, (List<Object>) value, entityType));
        } else if (value instanceof Map) {
            e.addProperty(createComplexProperty(name, (Map<String, Object>) value));
        } else if (entityType.getProperty(name).getType() instanceof EdmDate) {
            Date date = DatatypeConverter.parseDateTime((String) value).getTime();
            e.addProperty(createPrimitiveProperty(name, date));
        }
        else {
            e.addProperty(createPrimitiveProperty(name, value));
        }
    }

    private Property createPrimitiveProperty(String name, Object value) {
        return new Property(null, name, ValueType.PRIMITIVE, value);
    }

    private Property createComplexProperty(String name, Map<String, Object> value) {
        ComplexValue complexValue = createComplexValue(value);
        return new Property(null, name, ValueType.COMPLEX, complexValue);
    }

    @SuppressWarnings("unchecked")
    private Property createPropertyList(String name, List<Object> valueObject,
            ElasticEdmEntityType entityType) {
        ValueType valueType;
        EdmTypeKind propertyKind = entityType.getProperty(name).getType().getKind();
        if (propertyKind == EdmTypeKind.COMPLEX) {
            valueType = ValueType.COLLECTION_COMPLEX;
        } else {
            valueType = ValueType.COLLECTION_PRIMITIVE;
        }
        List<Object> properties = new ArrayList<>();
        for (Object value : valueObject) {
            if (value instanceof Map) {
                properties.add(createComplexValue((Map<String, Object>) value));
            } else {
                properties.add(value);
            }
        }
        return new Property(null, name, valueType, properties);
    }

    private ComplexValue createComplexValue(Map<String, Object> complexObject) {
        ComplexValue complexValue = new ComplexValue();
        for (Map.Entry<String, Object> entry : complexObject.entrySet()) {
            complexValue.getValue()
                    .add(new Property(null, entry.getKey(), ValueType.PRIMITIVE, entry.getValue()));
        }
        return complexValue;
    }

    public UriInfo getUriInfo() {
        return uriInfo;
    }

    public ElasticOData getOdata() {
        return odata;
    }

    public Client getClient() {
        return client;
    }

    public String getRawBaseUri() {
        return rawBaseUri;
    }

    public ElasticServiceMetadata getServiceMetadata() {
        return serviceMetadata;
    }

    public ContentType getResponseFormat() {
        return responseFormat;
    }

    /**
     * Encapsulates Query builder needed for getting the data from ES and last
     * URI entity set, needed for serializing the response.
     */
    protected class QueryWithEntity {
        private ElasticEdmEntitySet entitySet;
        private ESQueryBuilder query;

        public QueryWithEntity(ElasticEdmEntitySet entitySet, ESQueryBuilder query) {
            this.entitySet = entitySet;
            this.query = query;
        }

        public ElasticEdmEntitySet getEntitySet() {
            return entitySet;
        }

        public ESQueryBuilder getQuery() {
            return query;
        }
    }

}
