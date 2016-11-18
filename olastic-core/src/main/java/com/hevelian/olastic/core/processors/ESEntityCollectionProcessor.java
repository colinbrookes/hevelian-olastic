package com.hevelian.olastic.core.processors;


import com.hevelian.olastic.core.processors.data.DataRetriever;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.*;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.elasticsearch.client.Client;

/**
 * Processes entity collection.
 */
public class ESEntityCollectionProcessor implements EntityCollectionProcessor {
    private Client client;
	private OData odata;
	private ServiceMetadata serviceMetadata;

	public ESEntityCollectionProcessor(Client client) {
		this.client = client;
	}

	@Override
	public void init(OData odata, ServiceMetadata serviceMetadata) {
		this.odata = odata;
		this.serviceMetadata = serviceMetadata;
	}

	@Override
	public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
			ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
        DataRetriever dataRetriever = new DataRetriever(uriInfo, odata, client, request.getRawBaseUri(),
                serviceMetadata, responseFormat);

        SerializerResult serializerResult = dataRetriever.getSerializedData();
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }

}
