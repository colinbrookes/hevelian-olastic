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
 * Supported items for now:
 * 1. one 'groupby' for multiple fields;
 * 2. metrics aggregations;
 * 3. one 'groupby' for multiple fields with metrics aggregations;
 * 4. simple entity collections.
 * 
 * @author rdidyk
 */
public class CollectionProcessor extends ESEntityCollectionProcessor {

    private ElasticOData odata;
    private ElasticServiceMetadata serviceMetadata;

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        AbstractESCollectionProcessor collectionProcessor = getCollectionReadProcessor(uriInfo);
        collectionProcessor.init(odata, serviceMetadata);
        collectionProcessor.read(request, response, uriInfo, responseFormat);
    }

    /**
     * Gets specific collection reader based on items from apply option in URL.
     * 
     * @param uriInfo
     *            URI info
     * @return reader to get data
     * @throws ODataApplicationException
     *             if any error occurred
     */
    protected AbstractESCollectionProcessor getCollectionReadProcessor(UriInfo uriInfo)
            throws ODataApplicationException {
        ApplyOption applyOption = uriInfo.getApplyOption();
        List<GroupBy> groupByItems = getGroupByItems(applyOption);
        GroupBy groupBy = null;
        if (!groupByItems.isEmpty()) {
            if (groupByItems.size() > 1) {
                throwNotImplemented("Combining Transformations per Group is not supported.");
            }
            groupBy = groupByItems.get(0);
        }
        List<Aggregate> aggregations = getAggregations(applyOption);

        // Metrics aggregations only
        if (groupBy == null && !aggregations.isEmpty()) {
            return new MetricsAggregationsProccessor();
        } // Buckets aggregation only
        else if (groupBy != null && aggregations.isEmpty()) {
            return new BucketsAggegationsProcessor();
        } // Pipeline aggregation
        else if (groupBy != null && !aggregations.isEmpty()) {
            return throwNotImplemented(
                    "Aggregation for grouped and aggregated data is not implemented.");
        } else {
            // TODO Implement support of another items.
            return new EntityCollectionProcessor();
        }
    }

}
