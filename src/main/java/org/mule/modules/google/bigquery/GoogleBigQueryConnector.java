/**
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.modules.google.bigquery;

import org.mule.api.annotations.ConnectionIdentifier;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Connect;
import org.mule.api.annotations.Disconnect;
import org.mule.api.annotations.ValidateConnection;
import org.mule.api.annotations.param.ConnectionKey;
import org.mule.api.annotations.param.Default;
import org.mule.api.ConnectionException;
import org.mule.api.ConnectionExceptionCode;
import org.mule.api.annotations.display.Password;
import org.mule.api.annotations.Processor;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Cloud connector for the Google BigQuery API v3 using OAuth2 for initialization.
 * Uses OAuth2 for authentication
 *
 * @author MuleSoft, Inc.
 */
@Connector(name = "google-bigquery", schemaVersion = "1.0", friendlyName = "Google BigQuery", minMuleVersion = "3.5")
public class GoogleBigQueryConnector {

	private static Logger logger = LoggerFactory.getLogger(GoogleBigQueryConnector.class);	
	@SuppressWarnings("rawtypes")
	private static final List SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);
	private static final HttpTransport TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	// Credentials
	private GoogleCredential credential;
	
	// Big Query
	private Bigquery bigQuery;
	
	/**
	 * 
	 * @param applicationName
	 * @param clientId
	 * @param scope
	 * @param privateKeyP12File
	 * 
	 * Initiate connection
	 */
	@SuppressWarnings("unchecked")
	@Connect
	public void connect(@ConnectionKey String applicationName, @Password String clientId, String privateKeyP12File) 
		throws ConnectionException {
		
		logger.info(String.format("Logging into Google BigQuery application %s.", applicationName));
		
		try {
		    credential = new GoogleCredential.Builder().setTransport(TRANSPORT)
		            .setJsonFactory(JSON_FACTORY)
		            .setServiceAccountId(clientId)
		            .setServiceAccountScopes(SCOPES)
		            .setServiceAccountPrivateKeyFromP12File(new File(privateKeyP12File))
		            .build();

	        bigQuery = new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential)
	            .setApplicationName(applicationName)
	            .setHttpRequestInitializer(credential).build();
			
		}
		catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
			throw new ConnectionException(ConnectionExceptionCode.UNKNOWN, null, ex.getMessage(), ex);
		}
		
	}
	
	/**
	 * Disconnect
	 * 
	 * @throws ConnectionException
	 */
	@Disconnect
	public void disconnect() throws ConnectionException {		
		try {
			bigQuery = null;
		}
		catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
			throw new ConnectionException(ConnectionExceptionCode.UNKNOWN, null, ex.getMessage(), ex);
		}
	}

	/**
	 * Validate connection
	 */
	@ValidateConnection
	public boolean isConnected()
	{
		boolean isConnected = false;
		if (bigQuery != null)
			isConnected = true;
		
		return isConnected;
	}

	/**
	 * Session Access Token
	 */
	@ConnectionIdentifier
	public String connectionId() {
		String connectionId = credential.getAccessToken();
		logger.info("Connection Identifier: " + connectionId);
		return connectionId;
	}
		
	/**
	 * Streaming into Table. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:insert-all}
	 * 
	 * @param datasetId Dataset Id
	 * @param projectId Project Id
	 * @param tableId Table Id
	 * @param content Content to be inserted
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@Processor	
	public String insertAll(String datasetId, String projectId, String tableId, 
			@Default("#[payload]") java.util.Map<String, Object> content) {
		
		String response = null;
		TableDataInsertAllRequest insertAllRequest = new TableDataInsertAllRequest();

		insertAllRequest.setKind((String) content.get("kind"));
		Boolean skipInvalidRows = (Boolean) content.get("skipInvalidRows");
		if (skipInvalidRows != null)
			insertAllRequest.setSkipInvalidRows(skipInvalidRows);
		Boolean ignoreUnknownValues = (Boolean) content.get("ignoreUnknownValues");
		if (ignoreUnknownValues != null)
			insertAllRequest.setIgnoreUnknownValues(ignoreUnknownValues);

		@SuppressWarnings("rawtypes")
		List<Map> rows = (List<Map>) content.get("rows");
		List<TableDataInsertAllRequest.Rows> tableRows = new ArrayList<TableDataInsertAllRequest.Rows>();
		
		for (@SuppressWarnings("rawtypes") Map row: rows) {
			TableDataInsertAllRequest.Rows requestRows = new TableDataInsertAllRequest.Rows();
			String insertId = (String) row.get("insertId");
			if (insertId != null)
				requestRows.setInsertId(insertId);
			requestRows.setJson((Map<String,Object>) row.get("json"));
			tableRows.add(requestRows);
		}
				
		try {
			response = bigQuery.tabledata().insertAll(datasetId, projectId, tableId, insertAllRequest).getLastStatusMessage();
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error streaming into table%s.", ioe.toString()), ioe.getCause());
		}
		
		return response;
	}

}
