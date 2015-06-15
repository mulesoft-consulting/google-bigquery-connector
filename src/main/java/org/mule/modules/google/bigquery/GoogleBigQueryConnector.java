/**
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.modules.google.bigquery;

import org.apache.commons.io.IOUtils;
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
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableDataList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
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
	 * @param serviceAccount
	 * @param scope
	 * @param privateKeyP12File
	 * 
	 * Initiate connection
	 */
	@SuppressWarnings("unchecked")
	@Connect
	public void connect(@ConnectionKey String applicationName, @Password String serviceAccount, String privateKeyP12File) 
		throws ConnectionException {
		
		logger.info(String.format("Logging into Google BigQuery application %s.", applicationName));
		
		try {
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(privateKeyP12File);
			File p12File = File.createTempFile("privateKey", ".p12");
			OutputStream out = new FileOutputStream(p12File);
			IOUtils.copy(in, out);
			out.close();

		    credential = new GoogleCredential.Builder().setTransport(TRANSPORT)
		            .setJsonFactory(JSON_FACTORY)
		            .setServiceAccountId(serviceAccount)
		            .setServiceAccountScopes(SCOPES)
		            .setServiceAccountPrivateKeyFromP12File(p12File)
		            .build();
		    
		    credential.refreshToken();
		    logger.trace("Assess token: " + credential.getAccessToken());
		    logger.trace("Refresh token: " + credential.getRefreshToken());
		    
	        bigQuery = new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential)
	            .setApplicationName(applicationName)
	            .setHttpRequestInitializer(credential).build();
	        logger.info("BigQuery client created: " + bigQuery.toString());
			
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
	
		logger.info("Insert all payload:\n" + content);
		
		String strResponse = null;
		
		TableDataInsertAllRequest insertAllRequest = new TableDataInsertAllRequest();
		TableDataInsertAllResponse response = null;

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
		
		// Add rows
		insertAllRequest.setRows(tableRows);
			
		try {
			logger.trace("InsertAllRequest:\n" + insertAllRequest.toPrettyString());
			response = bigQuery.tabledata().insertAll(datasetId, projectId, tableId, insertAllRequest).execute();
			strResponse = response.toPrettyString();

			if (response != null) {
				List<InsertErrors> errorsList = response.getInsertErrors();
				for (InsertErrors errors: errorsList) {
					logger.error("Errors: " + errors.toPrettyString());
				}
			}
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error streaming into table%s.", ioe.toString()), ioe.getCause());
		}
		
		return strResponse;
	}

	/**
	 * Streaming into Table. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:list-all}
	 * 
	 * @param datasetId Dataset Id
	 * @param projectId Project Id
	 * @param tableId Table Id
	 * @return
	 */
	@Processor	
	public TableDataList listAll(String datasetId, String projectId, String tableId) {

		TableDataList list = null;
		
		try {
			logger.info("Listing: " + datasetId + " : " + projectId + " : " + tableId);
			list = bigQuery.tabledata().list(datasetId, projectId, tableId).execute();
			if (list != null)
				logger.info("List All response:\n" + list.toPrettyString());
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error listing table%s.", ioe.toString()), ioe.getCause());
		}
				
		return list;
		
	}
	
}
