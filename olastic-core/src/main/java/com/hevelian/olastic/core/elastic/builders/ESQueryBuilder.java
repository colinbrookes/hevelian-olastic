package com.hevelian.olastic.core.elastic.builders;

import com.hevelian.olastic.core.edm.ElasticEdmEntityType;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.*;
import org.apache.olingo.server.core.uri.UriResourceNavigationPropertyImpl;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

/**
 * Non-final builder class to create to Elasticsearch with possibility to
 * override behavior in case someone needs own way to build query.
 * 
 * @author rdidyk
 *
 * @param <T>
 *            builder instance class
 */
public class ESQueryBuilder<T extends ESQueryBuilder<T>> {

    protected BoolQueryBuilder query;
    protected QueryBuilder parentChildQuery;
    protected List<QueryBuilder> filters;

    /**
     * Default constructor.
     */
    public ESQueryBuilder() {
        this.query = QueryBuilders.boolQuery();
        this.filters = new ArrayList<>();
    }

    /**
     * Method adds query depending on current and next URI resource segment.
     * 
     * @param segment
     *            current segment
     * @param nextSegment
     *            next segment
     * @return casted queryBuilder
     * @throws ODataApplicationException
     *             if any error occurred
     */
    @SuppressWarnings("unchecked")
    public T addSegmentQuery(UriResource segment, UriResource nextSegment)
            throws ODataApplicationException {
        ElasticEdmEntityType type = (ElasticEdmEntityType) ((UriResourcePartTyped) segment)
                .getType();
        String eType = type.getEType();
        List<String> ids = collectIds(segment);
        if (nextSegment == null) {
            addIdQuery(eType, ids);
        } else {
            if (nextSegment.getKind() == UriResourceKind.primitiveProperty) {
                addIdQuery(eType, ids);
            } else {
                if (((UriResourceNavigationPropertyImpl) nextSegment).getProperty()
                        .isCollection()) {
                    addParentQuery(eType, ids);
                } else {
                    addChildQuery(eType, ids);
                }
            }
        }
        return (T) this;
    }

    /**
     * Retrieves ids from uri resource part.
     *
     * @param segment
     *            uri resource part
     * @return ids list
     * @throws ODataApplicationException odata app exception
     */
    protected List<String> collectIds(UriResource segment) throws ODataApplicationException {
        List<UriParameter> keyPredicates;
        if (segment instanceof UriResourceNavigation) {
            keyPredicates = ((UriResourceNavigation) segment).getKeyPredicates();
        } else {
            keyPredicates = ((UriResourceEntitySet) segment).getKeyPredicates();
        }
        if (keyPredicates.size() > 1) {
            throwNotImplemented("Composite Keys are not supported");
        }
        return keyPredicates.stream().map(param -> param.getText().replaceAll("\'", ""))
                .collect(Collectors.toList());
    }

    /**
     * Adds a query that <b>must</b> appear in the matching documents but will
     * not contribute to scoring.
     * 
     * @param query
     *            query to add
     * @return builder instance
     */
    @SuppressWarnings("unchecked")
    public T addFilter(QueryBuilder query) {
        if (query != null) {
            filters.add(query);
        }
        return (T) this;
    }

    /**
     * Adds new level of parent query.
     * 
     * @param type
     *            parent type
     * @param ids
     *            list of ids of parent documents we are looking for
     */
    private void addParentQuery(String type, List<String> ids) {
        QueryBuilder parentQuery = ids.isEmpty() ? QueryBuilders.matchAllQuery()
                : buildIdQuery(type, ids);
        QueryBuilder resultQuery = getParentChildResultQuery(parentQuery);
        parentChildQuery = QueryBuilders.hasParentQuery(type, resultQuery, false);
    }

    /**
     * Adds new level of child query.
     * 
     * @param type
     *            child type
     * @param ids
     *            list of ids of child documents we are looking for
     */
    private void addChildQuery(String type, List<String> ids) {
        QueryBuilder childQuery = ids.isEmpty() ? QueryBuilders.matchAllQuery()
                : buildIdQuery(type, ids);
        QueryBuilder resultQuery = getParentChildResultQuery(childQuery);
        parentChildQuery = QueryBuilders.hasChildQuery(type, resultQuery, ScoreMode.None);
    }

    /**
     * Builds must query with existing {@link #parentChildQuery} and new query,
     * or just returns new query, if {@link #parentChildQuery} is null. Note: we
     * can't initialize {@link #parentChildQuery} in the beginning, because we
     * don't know what type it will be: has_parent or has_child.
     * 
     * @param query
     *            new query
     * @return raw es query
     */
    protected QueryBuilder getParentChildResultQuery(QueryBuilder query) {
        return parentChildQuery != null
                ? QueryBuilders.boolQuery().must(parentChildQuery).must(query) : query;
    }

    /**
     * Adds ids query to the current level.
     * 
     * @param type
     *            type
     * @param ids
     *            list of ids
     */
    protected void addIdQuery(String type, List<String> ids) {
        if (!ids.isEmpty()) {
            query.must(buildIdQuery(type, ids));
        }
    }

    /**
     * Creates query for specific id of type.
     * 
     * @param type
     *            type name
     * @param ids
     *            list of ids
     * @return query for id
     */
    protected QueryBuilder buildIdQuery(String type, List<String> ids) {
        return new IdsQueryBuilder().types(type).addIds(ids.toArray(new String[1]));
    }

    /**
     * Returns raw Elasticsearch query.
     * 
     * @return query builder
     */
    public QueryBuilder build() {
        BoolQueryBuilder resultQuery = QueryBuilders.boolQuery();
        if (query.hasClauses()) {
            resultQuery.must(query);
        }
        if (parentChildQuery != null) {
            resultQuery.must(parentChildQuery);
        }
        filters.forEach(resultQuery::filter);
        return resultQuery;
    }

}
