package com.hevelian.olastic.core.processors.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.ApplyItem;
import org.apache.olingo.server.api.uri.queryoption.ApplyOption;
import org.apache.olingo.server.api.uri.queryoption.apply.GroupBy;
import org.elasticsearch.client.Client;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.ElasticServiceMetadata;
import com.hevelian.olastic.core.processors.ESEntityCollectionProcessor;
import com.hevelian.olastic.core.processors.data.DataRetriever;
import com.hevelian.olastic.core.processors.data.EntityCollectionRetriever;
import com.hevelian.olastic.core.processors.data.GroupByCollectionRetriever;

/**
 * Processes entity collection.
 */
public class ESEntityCollectionProcessorImpl extends ESEntityCollectionProcessor {

    private Client client;
    private ElasticOData odata;
    private ElasticServiceMetadata serviceMetadata;

    public ESEntityCollectionProcessorImpl(Client client) {
        this.client = client;
    }

    @Override
    public void init(ElasticOData odata, ElasticServiceMetadata serviceMetadata) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        DataRetriever dataRetriever;
        List<GroupBy> groupByItems = getGroupByItems(uriInfo.getApplyOption());
        if (!groupByItems.isEmpty()) {
            if (groupByItems.size() > 1) {
                throw new ODataApplicationException("Only one 'groupBy' is supported.",
                        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
            }
            dataRetriever = new GroupByCollectionRetriever(uriInfo, odata, client,
                    request.getRawBaseUri(), serviceMetadata, responseFormat, groupByItems.get(0));
        } else {
            dataRetriever = new EntityCollectionRetriever(uriInfo, odata, client,
                    request.getRawBaseUri(), serviceMetadata, responseFormat);
        }

        SerializerResult serializerResult = dataRetriever.getSerializedData();
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }

    private List<GroupBy> getGroupByItems(ApplyOption applyOption) {
        List<GroupBy> groupByList = new ArrayList<>();
        if (applyOption == null) {
            return groupByList;
        }
        for (ApplyItem applyItem : applyOption.getApplyItems()) {
            if (applyItem.getKind() == ApplyItem.Kind.GROUP_BY) {
                groupByList.add((GroupBy) applyItem);
            }
        }
        return groupByList;
    }

}
