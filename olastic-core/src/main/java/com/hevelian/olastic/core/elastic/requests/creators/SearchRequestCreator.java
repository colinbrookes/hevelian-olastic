package com.hevelian.olastic.core.elastic.requests.creators;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceKind;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;

import com.hevelian.olastic.core.edm.ElasticEdmEntitySet;
import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import com.hevelian.olastic.core.elastic.builders.ESQueryBuilder;
import com.hevelian.olastic.core.elastic.queries.Query;
import com.hevelian.olastic.core.elastic.queries.SearchQuery;
import com.hevelian.olastic.core.elastic.requests.ESRequest;
import com.hevelian.olastic.core.elastic.requests.SearchRequest;

/**
 * Class responsible for creating {@link SearchRequest} instance.
 * 
 * @author rdidyk
 */
public class SearchRequestCreator extends AbstractRequestCreator {

    /**
     * Constructor to initialize default ES query builder.
     */
    public SearchRequestCreator() {
        this(new ESQueryBuilder<>());
    }

    /**
     * Constructor to initialize ES query builder.
     * 
     * @param queryBuilder
     *            ES query builder
     */
    public SearchRequestCreator(ESQueryBuilder<?> queryBuilder) {
        super(queryBuilder);
    }

    @Override
    public ESRequest create(UriInfo uriInfo) throws ODataApplicationException {
        ESRequest baseRequestInfo = getBaseRequestInfo(uriInfo);
        Query baseQuery = baseRequestInfo.getQuery();
        ElasticEdmEntitySet entitySet = baseRequestInfo.getEntitySet();
        ElasticEdmEntityType entityType = entitySet.getEntityType();

        Set<String> fields = getSelectList(uriInfo).stream()
                .map(field -> entityType.getEProperties().get(field).getEField())
                .collect(Collectors.toSet());
        SearchQuery searchQuery = new SearchQuery(baseQuery.getIndex(), baseQuery.getType(),
                baseQuery.getQueryBuilder(), fields);
        return new SearchRequest(searchQuery, entitySet, getPagination(uriInfo));
    }

    /**
     * Returns the list of fields from URL.
     *
     * @return fields fields from URL
     */
    protected List<String> getSelectList(UriInfo uriInfo) {
        List<String> result = new ArrayList<>();
        SelectOption selectOption = uriInfo.getSelectOption();
        if (selectOption != null) {
            List<SelectItem> selectItems = selectOption.getSelectItems();
            for (SelectItem selectItem : selectItems) {
                List<UriResource> selectParts = selectItem.getResourcePath().getUriResourceParts();
                String fieldName = selectParts.get(selectParts.size() - 1).getSegmentValue();
                result.add(fieldName);
            }
        } else {
            List<UriResource> resourceParts = uriInfo.getUriResourceParts();
            if (resourceParts.size() > 1) {
                UriResource lastResource = resourceParts.get(resourceParts.size() - 1);
                if (lastResource.getKind() == UriResourceKind.primitiveProperty) {
                    result.add(((UriResourceProperty) lastResource).getProperty().getName());
                }
            }
        }
        return result;
    }

}