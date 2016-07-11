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
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.googleapis.services.json.CommonGoogleJsonClientRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
	private static final List SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY, BigqueryScopes.BIGQUERY_INSERTDATA, BigqueryScopes.CLOUD_PLATFORM);
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();

	private static HttpTransport httpTransport = null;
	
	private static Gson gson = new Gson();

	// Credentials
	private GoogleCredential credential;
	
	// Big Query
	private Bigquery bigQuery;
	
	// Application name
	private String applicationName;
	
	/**
	 * 
	 * @param applicationName
	 * @param serviceAccount
	 * @param scope
	 * @param privateKeyFile
	 * 
	 * Initiate connection
	 */
	@SuppressWarnings("unchecked")
	@Connect
	public void connect(@ConnectionKey String applicationName, @Password String serviceAccount, String privateKeyFile,  
			@Password String storePass, String alias, @Password String keyPass) 
		throws ConnectionException {
		
		logger.info(String.format("Logging into Google BigQuery application %s :: %s :: %s", applicationName, privateKeyFile, alias));
		this.applicationName = applicationName;
		
		try {
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(privateKeyFile);
			logger.info("Key file loaded " + in.available());
			PrivateKey key = SecurityUtils.loadPrivateKeyFromKeyStore(SecurityUtils.getJavaKeyStore(), in, storePass, alias, keyPass);
			logger.trace("Key size: "+key.getEncoded().length);
			logger.trace("Key algorithm: "+key.getAlgorithm());
			
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			
		    credential = new GoogleCredential.Builder().setTransport(httpTransport)
		            .setJsonFactory(JSON_FACTORY)
		            .setServiceAccountId(serviceAccount)
		            .setServiceAccountScopes(SCOPES)
		            .setServiceAccountPrivateKey(key)
		            .build();
		    
		    refreshBigQueryClient();
			
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
			@Default("#[payload]") java.util.Map<String, Object> content, @Default("0") long throttle) {
	
		logger.trace("Insert all payload:\n" + content);
		
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
		// createDisposition
		String createDisposition = (String) content.get("createDisposition");
		if (createDisposition != null)
			insertAllRequest.set("createDisposition", createDisposition);
		// createDisposition
		String writeDisposition = (String) content.get("writeDisposition");
		if (writeDisposition != null)
			insertAllRequest.set("writeDisposition", writeDisposition);		

		@SuppressWarnings("rawtypes")
		List<Map> rows = (List<Map>) content.get("rows");
		List<TableDataInsertAllRequest.Rows> tableRows = new ArrayList<TableDataInsertAllRequest.Rows>();
		
		for (@SuppressWarnings("rawtypes") Map row: rows) {
			TableDataInsertAllRequest.Rows requestRows = new TableDataInsertAllRequest.Rows();
			String insertId = (String) row.get("insertId");
			if (insertId != null)
				requestRows.setInsertId(insertId);
			else
				requestRows.setInsertId(String.valueOf(System.nanoTime()));
			TableRow tableRow = new TableRow();
			
			tableRow.putAll((Map<String,Object>) row.get("json"));
			requestRows.setJson(tableRow);
			logger.trace("Row: " + gson.toJson(requestRows));
			tableRows.add(requestRows);
		}
		
		// Add rows
		insertAllRequest.setRows(tableRows);
			
		try {
			refreshBigQueryClient();
			logger.trace("InsertAllRequest:\n" + gson.toJson(insertAllRequest));
			try {
				Thread.sleep(throttle);
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}
			response = bigQuery.tabledata().insertAll(projectId, datasetId, tableId, insertAllRequest).execute();
			strResponse = response.toPrettyString();

			if (response != null) {
				List<InsertErrors> errorsList = response.getInsertErrors();
				if (errorsList != null) {
					for (InsertErrors errors: errorsList) {
						logger.error("Errors: " + errors.toPrettyString());
					}
				}
			}
		}
		catch (GoogleJsonResponseException gjre) {
			gjre.printStackTrace();
			logger.error(gjre.getMessage());
			throw new RuntimeException(String.format("Error streaming into table%s.", gjre.toString()), gjre.getCause());
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error streaming into table%s.", ioe.toString()), ioe.getCause());
		}
		
		return strResponse;
	}

	/**
	 * Listing all from Table. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:list-all}
	 * 
	 * @param projectId Project Id
	 * @param datasetId Dataset Id
	 * @param tableId Table Id
	 * @return
	 */
	@Processor	
	public TableDataList listAll(String datasetId, String projectId, String tableId) {

		TableDataList list = null;
		
		try {
			refreshBigQueryClient();
			logger.info("Listing: " + projectId + " : " + datasetId + " : " + tableId);
			list = bigQuery.tabledata().list(projectId, datasetId, tableId).execute();
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

	/**
	 * Deleting Table. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:delete-table}
	 * 
	 * @param projectId Project Id
	 * @param datasetId Dataset Id
	 * @param tableId Table Id
	 * @return
	 */
	@Processor
	public void deleteTable(String datasetId, String projectId, String tableId) {
		
		try {
			refreshBigQueryClient();
			logger.info("Deleting: " + projectId + " : " + datasetId + " : " + tableId);
			bigQuery.tables().delete(projectId, datasetId, tableId).execute();
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error deleting table%s.", ioe.toString()), ioe.getCause());
		}		
		
	}
	
	/**
	 * Inserting Empty Table. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:create-table}
	 * 
	 * @param projectId Project Id
	 * @param datasetId Dataset Id
	 * @param tableId Table Id
	 * @param content Schema of table in map format
	 * @return
	 */
	@Processor 
	public void createTable(String datasetId, String projectId, String tableId, 
			@Default("#[payload]") java.util.Map<String, Object> content) {
		
		@SuppressWarnings("unchecked")
		ArrayList<HashMap<String,String>> fieldmaps = (ArrayList<HashMap<String,String>>) content.get("fields");
		ArrayList<TableFieldSchema> fieldsSchema = new ArrayList<TableFieldSchema>();
		
		for (@SuppressWarnings("rawtypes") HashMap fieldmap : fieldmaps) {
			TableFieldSchema tfSchema = new TableFieldSchema();
			tfSchema.setName((String) fieldmap.get("name"));
			tfSchema.setMode((String) fieldmap.get("mode"));
			tfSchema.setType((String) fieldmap.get("type"));
			tfSchema.setDescription((String) fieldmap.get("description"));
			logger.info("Field schema: " + tfSchema);
			// Add
			fieldsSchema.add(tfSchema);
		}
		
		TableSchema tableSchema = new TableSchema();
		tableSchema.setFields(fieldsSchema);
		
		TableReference tableReference = new TableReference();
		tableReference.setDatasetId(datasetId);
		tableReference.setProjectId(projectId);
		tableReference.setTableId(tableId);
		
		Table table = new Table();
		table.setSchema(tableSchema);
		table.setTableReference(tableReference);
		
		try {
			refreshBigQueryClient();
			logger.info("Inserting: " + projectId + " : " + datasetId + " : " + tableId);
			bigQuery.tables().insert(projectId, datasetId, table).execute();
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error insert table%s.", ioe.toString()), ioe.getCause());
		}			
		
		return;
	}
	
	/**
	 * Listing all tables. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:list-all-tables}
	 * 
	 * @param projectId Project Id
	 * @param datasetId Dataset Id
	 * @return
	 */
	@Processor	
	public List<TableList.Tables> listAllTables(String projectId, String datasetId) {

		TableList tableList = null;
		
		try {
			refreshBigQueryClient();
			logger.info("Listing Tables for project " +  projectId + " :: dataset " + datasetId);
			tableList = bigQuery.tables().list(projectId, datasetId).execute();
			if (tableList != null)
				logger.info("List All Tables response:\n" + tableList.toPrettyString());
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error listing Tables%s.", ioe.toString()), ioe.getCause());
		}
				
		return tableList.getTables();
		
	}

	/**
	 * Listing all table fields. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:list-all-tablefields}
	 * 
	 * @param projectId Project Id
	 * @return
	 */
	@Processor	
	public List<TableFieldSchema> listAllTableFields(String projectId, String datasetId, String tableId) {

		List<TableFieldSchema> fields = null;
		
		try {
			refreshBigQueryClient();
			logger.info("Listing fields for project " + projectId + " :: dataset " + datasetId + " :: table " + tableId);
			fields = bigQuery.tables().get(projectId, datasetId, tableId).execute().getSchema().getFields();
			if (fields != null)
				logger.info("List All Fields response:\n" + fields);
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error listing datasets%s.", ioe.toString()), ioe.getCause());
		}
				
		return fields;
		
	}

	/**
	 * Listing all datasets. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:list-all-datasets}
	 * 
	 * @param projectId Project Id
	 * @return
	 */
	@Processor	
	public List<DatasetList.Datasets> listAllDatasets(String projectId) {

		DatasetList datasetList = null;
		
		try {
			refreshBigQueryClient();
			logger.info("Listing datasets for project " + projectId);
			datasetList = bigQuery.datasets().list(projectId).execute();
			if (datasetList != null)
				logger.info("List All Dataset response:\n" + datasetList.toPrettyString());
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error listing datasets%s.", ioe.toString()), ioe.getCause());
		}
				
		return datasetList.getDatasets();
		
	}

	/**
	 * Listing all projects. 
	 * 
	 * {@sample.xml ../../../doc/google-bigquery-connector.xml.sample google-bigquery:list-all-projects}
	 * 
	 * @return
	 */
	@Processor	
	public List<ProjectList.Projects> listAllProjects() {

		ProjectList projectList = null;
		
		try {
			refreshBigQueryClient();
			logger.info("Listing projects...");
			projectList = bigQuery.projects().list().execute();
			if (projectList != null)
				logger.info("List All Projects response:\n" + projectList.toPrettyString());
		}
		catch (java.io.IOException ioe) {
			ioe.printStackTrace();
			logger.error(ioe.getMessage());
			throw new RuntimeException(String.format("Error listing projects%s.", ioe.toString()), ioe.getCause());
		}
				
		return projectList.getProjects();
		
	}

	/**
	 * Refresh credentials
	 */
	private void refreshBigQueryClient() throws IOException {
	    credential.refreshToken();
	    logger.trace("Assess token: " + credential.getAccessToken());
	    logger.trace("Refresh token: " + credential.getRefreshToken());
	    
        bigQuery = new Bigquery.Builder(httpTransport, JSON_FACTORY, credential)
			.setGoogleClientRequestInitializer(new CommonGoogleJsonClientRequestInitializer() {
				@SuppressWarnings("unused")
				public void initialize(@SuppressWarnings("rawtypes") AbstractGoogleJsonClientRequest request) {
			        @SuppressWarnings("rawtypes")
					BigqueryRequest bigqueryRequest = (BigqueryRequest) request;
			        bigqueryRequest.setPrettyPrint(true);
				}
			})
            .setApplicationName(this.applicationName)
            .setHttpRequestInitializer(credential).build();
        logger.trace("BigQuery client created: " + bigQuery.toString());		
	}
}
