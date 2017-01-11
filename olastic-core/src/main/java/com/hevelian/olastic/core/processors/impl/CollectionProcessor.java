package com.hevelian.olastic.core.processors.impl;

import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getAggregations;
import static com.hevelian.olastic.core.utils.ApplyOptionUtils.getGroupByItems;
import static com.hevelian.olastic.core.utils.ProcessorUtils.throwNotImplemented;

import java.util.List;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.apply.Aggregate;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.processors.AbstractESCollectionProcessor;
import com.hevelian.olastic.core.processors.ESEntityCollectionProcessor;

/**
 * Custom Elastic processor for handling all request to retrieve data from
 * collection of entities.
 * 
 * @author rdidyk
 */
public class CollectionProcessor extends ESEntityCollectionProcessor {

    private ElasticOData odata;
    private ElasticServiceMetadata serviceMetadata;

    private GroupBy groupBy;
    private List<Aggregate> aggregations;

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        initializeItems(uriInfo.getApplyOption());
        AbstractESCollectionProcessor collectionProcessor = getCollectionReadProcessor();
        collectionProcessor.init(odata, serviceMetadata);
        collectionProcessor.read(request, response, uriInfo, responseFormat);
    }

    /**
     * Method initializes all kind of supported items from {@link ApplyOption}
     * in URL. Supported items for now: 1. one 'groupby' for multiple fields; 2.
     * metrics aggregations; 3. one 'groupby' for multiple fields with metrics
     * aggregations; 4. simple entity collections.
     * 
     * @param applyOption
     *            apply option
     * @throws ODataApplicationException
     *             if any error occurred
     */
    private void initializeItems(ApplyOption applyOption) throws ODataApplicationException {
        List<GroupBy> groupByItems = getGroupByItems(applyOption);
        if (!groupByItems.isEmpty()) {
            if (groupByItems.size() > 1) {
                throwNotImplemented("Combining Transformations per Group is not supported.");
            }
            this.groupBy = groupByItems.get(0);
        }
        this.aggregations = getAggregations(applyOption);
    }

    /**
     * Gets specific collection reader based on items from apply option in URL.
     * 
     * @return reader to get data
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected AbstractESCollectionProcessor getCollectionReadProcessor()
            throws ODataApplicationException {
        if (isAggregateOnly()) {
            return new MetricsAggregationsProccessor();
        } else if (isGroupByOnly()) {
            return new BucketsAggegationsProcessor();
        } else if (isGroupByAndAggregate()) {
            return throwNotImplemented(
                    "Aggregation for grouped and aggregated data is not implemented.");
        } else {
            // TODO Implement support of another items.
            return new EntityCollectionProcessor();
        }
    }

    private boolean isAggregateOnly() {
        return groupBy == null && !aggregations.isEmpty();
    }

    private boolean isGroupByOnly() {
        return groupBy != null && aggregations.isEmpty();
    }

    private boolean isGroupByAndAggregate() {
        return groupBy != null && !aggregations.isEmpty();
    }

}
