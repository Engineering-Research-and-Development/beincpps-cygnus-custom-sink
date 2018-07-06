/**
 * Copyright 2015-2017 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FIWARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */

package com.telefonica.iot.cygnus.sinks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flume.Context;

import com.google.common.collect.ImmutableMap;
import com.telefonica.iot.cygnus.backends.postgresql.PostgreSQLBackendImpl4BEinCPPS;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextAttribute;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.errors.CygnusCappingError;
import com.telefonica.iot.cygnus.errors.CygnusExpiratingError;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.interceptors.NGSIEvent;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.utils.NGSICharsets;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.telefonica.iot.cygnus.utils.NGSIUtils;

/**
 *
 * @author hermanjunge

 Detailed documentation can be found at:
 https://github.com/telefonicaid/fiware-cygnus/blob/master/doc/design/NGSIPostgreSQLSink.md
 */
public class NGSIPostgreSQLSink4BEinCCPS extends NGSISink {

	private static final CygnusLogger LOGGER = new CygnusLogger(NGSIPostgreSQLSink4BEinCCPS.class);
	private String postgresqlHost;
	private String postgresqlPort;
	private String postgresqlDatabase;
	private String postgresqlUsername;
	private String postgresqlPassword;
	private PostgreSQLBackendImpl4BEinCPPS persistenceBackend;
	private boolean enableCache;

	/* 4 Mapping cb -> db */
	protected Map<String, AttributeMapping4BEinCPPS> attributesMapping;
	private String tableDefaultPrimaryKey;     
	/**/

	/**
	 * Constructor.
	 */
	public NGSIPostgreSQLSink4BEinCCPS() {
		super();
		attributesMapping = new HashMap<String, AttributeMapping4BEinCPPS>();
	} // NGSIPostgreSQLSink

	/**
	 * Gets the PostgreSQL host. It is protected due to it is only required for testing purposes.
	 * @return The PostgreSQL host
	 */
	protected String getPostgreSQLHost() {
		return postgresqlHost;
	} // getPostgreSQLHost

	/**
	 * Gets the PostgreSQL cache. It is protected due to it is only required for testing purposes.
	 * @return The PostgreSQL cache state
	 */
	protected boolean getEnableCache() {
		return enableCache;
	} // getPostgreSQLHost

	/**
	 * Gets the PostgreSQL port. It is protected due to it is only required for testing purposes.
	 * @return The PostgreSQL port
	 */
	protected String getPostgreSQLPort() {
		return postgresqlPort;
	} // getPostgreSQLPort

	/**
	 * Gets the PostgreSQL database. It is protected due to it is only required for testing purposes.
	 * @return The PostgreSQL database
	 */
	protected String getPostgreSQLDatabase() {
		return postgresqlDatabase;
	} // getPostgreSQLDatabase

	/**
	 * Gets the PostgreSQL username. It is protected due to it is only required for testing purposes.
	 * @return The PostgreSQL username
	 */
	protected String getPostgreSQLUsername() {
		return postgresqlUsername;
	} // getPostgreSQLUsername

	/**
	 * Gets the PostgreSQL password. It is protected due to it is only required for testing purposes.
	 * @return The PostgreSQL password
	 */
	protected String getPostgreSQLPassword() {
		return postgresqlPassword;
	} // getPostgreSQLPassword

	/**
	 * Returns the persistence backend. It is protected due to it is only required for testing purposes.
	 * @return The persistence backend
	 */
	protected PostgreSQLBackendImpl4BEinCPPS getPersistenceBackend() {
		return persistenceBackend;
	} // getPersistenceBackend

	/**
	 * Sets the persistence backend. It is protected due to it is only required for testing purposes.
	 * @param persistenceBackend
	 */
	protected void setPersistenceBackend(PostgreSQLBackendImpl4BEinCPPS persistenceBackend) {
		this.persistenceBackend = persistenceBackend;
	} // setPersistenceBackend

	@Override
	public void configure(Context context) {
		// Read NGSISink general configuration
		super.configure(context);

		// Impose enable lower case, since PostgreSQL only accepts lower case
		enableLowercase = true;

		postgresqlHost = context.getString("postgresql_host", "localhost");
		LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_host=" + postgresqlHost + ")");
		postgresqlPort = context.getString("postgresql_port", "5432");
		int intPort = Integer.parseInt(postgresqlPort);

		if ((intPort <= 0) || (intPort > 65535)) {
			invalidConfiguration = true;
			LOGGER.debug("[" + this.getName() + "] Invalid configuration (postgresql_port=" + postgresqlPort + ")"
					+ " -- Must be between 0 and 65535");
		} else {
			LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_port=" + postgresqlPort + ")");
		}  // if else

		postgresqlDatabase = context.getString("postgresql_database", "postgres");
		LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_database=" + postgresqlDatabase + ")");
		postgresqlUsername = context.getString("postgresql_username", "postgres");
		LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_username=" + postgresqlUsername + ")");
		// FIXME: postgresqlPassword should be read as a SHA1 and decoded here
		postgresqlPassword = context.getString("postgresql_password", "");
		LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_password=" + postgresqlPassword + ")");

		String enableCacheStr = context.getString("backend.enable_cache", "false");

		if (enableCacheStr.equals("true") || enableCacheStr.equals("false")) {
			enableCache = Boolean.valueOf(enableCacheStr);
			LOGGER.debug("[" + this.getName() + "] Reading configuration (backend.enable_cache=" + enableCache + ")");
		}  else {
			invalidConfiguration = true;
			LOGGER.debug("[" + this.getName() + "] Invalid configuration (backend.enable_cache="
					+ enableCache + ") -- Must be 'true' or 'false'");
		}  // if else

		tableDefaultPrimaryKey = context.getString("tableDefaultPrimaryKey", "ResourceID, timeStamp");
		LOGGER.debug("[" + this.getName() + "] Reading configuration (tableDefaultPrimaryKey=" + tableDefaultPrimaryKey + ")");
		
		ImmutableMap<String, String> configurationRows = context.getSubProperties("attribute-mapping.");
		LOGGER.debug("[" + this.getName() + "] Reading configuration (attribute-mapping=" + configurationRows.toString() + ")");

		for (Map.Entry<String, String> entry : configurationRows.entrySet()) {
			LOGGER.debug(entry.getKey() + "=" + entry.getValue());
			String s[] = entry.getKey().split("\\.");
			if(s.length != 2) {
				LOGGER.warn("attribute-mapping in wrong format " + entry.getKey() + ", s.length=" + s.length);
			} else {
				attributesMapping.put(entry.getKey() , new AttributeMapping4BEinCPPS(entry.getValue()));
			}
		}

		LOGGER.debug("attributesMapping.keySet()->" + attributesMapping.keySet());
	} // configure

	@Override
	public void start() {
		try {
			persistenceBackend = new PostgreSQLBackendImpl4BEinCPPS(postgresqlHost, postgresqlPort, postgresqlDatabase,
					postgresqlUsername, postgresqlPassword, enableCache);
		} catch (Exception e) {
			LOGGER.error("Error while creating the PostgreSQL persistence backend. Details="
					+ e.getMessage());
		} // try catch

		super.start();
		LOGGER.info("[" + this.getName() + "] Startup completed");
	} // start

	@Override
	void persistBatch(NGSIBatch batch) throws CygnusBadConfiguration, CygnusPersistenceError {
		if (batch == null) {
			LOGGER.debug("[" + this.getName() + "] Null batch, nothing to do");
			return;
		} // if

		// Iterate on the destinations
		batch.startIterator();

		while (batch.hasNext()) {
			String destination = batch.getNextDestination();
			LOGGER.debug("[" + this.getName() + "] Processing sub-batch regarding the "
					+ destination + " destination");

			// get the sub-batch for this destination
			ArrayList<NGSIEvent> events = batch.getNextEvents();

			// get an aggregator for this destination and initialize it
			PostgreSQLAggregator aggregator = getAggregator();
			aggregator.initialize(events.get(0));

			for (NGSIEvent event : events) {
				aggregator.aggregate(event);
			} // for

			// persist the fieldValues
			persistAggregation(aggregator);
			batch.setNextPersisted(true);
		} // for
	} // persistBatch

	@Override
	public void capRecords(NGSIBatch batch, long maxRecords) throws CygnusCappingError {
	} // capRecords

	@Override
	public void expirateRecords(long expirationTime) throws CygnusExpiratingError {
	} // expirateRecords

	public Map<String, AttributeMapping4BEinCPPS> getAttributesMapping() {
		return attributesMapping;
	}

	/**
	 * Class for aggregating fieldValues.
	 */
	private abstract class PostgreSQLAggregator {

		protected List<String> queries;

		protected String service;
		protected String schemaName;
		
		public PostgreSQLAggregator() {
			queries = new ArrayList<String>();
		} // PostgreSQLAggregator

		public List<String> getQueries() {
			return queries;
		} // getQueries

		public String getSchemaName() {
			return schemaName.toLowerCase();
		} // getDbName

		public void initialize(NGSIEvent event) throws CygnusBadConfiguration {
			System.out.println("in initialize->" + getAttributesMapping().entrySet());
			service = event.getServiceForNaming(enableNameMappings);
			schemaName = buildSchemaName(service);
		} // initialize

		public abstract void aggregate(NGSIEvent cygnusEvent);

	} // PostgreSQLAggregator

	private class Aggregator4BEinCPPS extends PostgreSQLAggregator {

		@Override
		public void aggregate(NGSIEvent cygnusEvent) {
			String type = cygnusEvent.getContextElement().getType();

			ArrayList<ContextAttribute> attributes = cygnusEvent.getContextElement().getAttributes();

			Map<String, List<AttributeMapping4BEinCPPS>> map4GenerateQuery = new HashMap<String, List<AttributeMapping4BEinCPPS>>();

			for (Iterator<ContextAttribute> iterator = attributes.iterator(); iterator.hasNext();) {
				ContextAttribute contextAttribute = (ContextAttribute) iterator.next();
				String key = type + "." + contextAttribute.getName();

				if(getAttributesMapping().containsKey(key)) {
					AttributeMapping4BEinCPPS attributeMapping = getAttributesMapping().get(key);
					String tableName = attributeMapping.getTableName();

					attributeMapping.setValue(contextAttribute.getContextValue(true));

					if(map4GenerateQuery.containsKey(tableName)) {
						map4GenerateQuery.get(tableName).add(attributeMapping);
					} else {
						ArrayList<AttributeMapping4BEinCPPS> l = new ArrayList<AttributeMapping4BEinCPPS>();
						l.add(attributeMapping);
						map4GenerateQuery.put(tableName, l);
					}
				}
			}

			String primaryKeyColumns = "\"" + tableDefaultPrimaryKey.replace(" ", "").replace(",", "\", \"") + "\"";
			
			//"\"ResourceID\", \"timeStamp\""; /* va spostato nel file di configurazione */

			Set<String> tablesToInsert = map4GenerateQuery.keySet();

			for (String tableCurr : tablesToInsert) {
				List<AttributeMapping4BEinCPPS> attrToAdd = map4GenerateQuery.get(tableCurr);

				StringBuffer columns = new StringBuffer(primaryKeyColumns);
				StringBuffer values = new StringBuffer("'" + cygnusEvent.getContextElement().getId() + "', '" +	cygnusEvent.getRecvTimeTs() + "'");

				for (AttributeMapping4BEinCPPS attributeToAddNow : attrToAdd) {
					columns.append(", \"" + attributeToAddNow.getColumnName() + "\"");
					values.append(", '" + attributeToAddNow.getValue().replace("\"", "") + "'");
				}

				String queryCurr = "INSERT INTO " + getSchemaName() + ".\"" + tableCurr + 
						"\" (" + columns.toString() + ") VALUES (" + values.toString() + ")";

				queries.add(queryCurr);
			}
		}
	} // Aggregator4BEinCPPS

	private PostgreSQLAggregator getAggregator() {
		return new Aggregator4BEinCPPS();
	} // getAggregator

	private void persistAggregation(PostgreSQLAggregator aggregator) throws CygnusPersistenceError {
		String schemaName = aggregator.getSchemaName();
		List<String> queries = aggregator.getQueries();
		try {
			if (aggregator instanceof Aggregator4BEinCPPS) {
				persistenceBackend.createSchema(schemaName);
                persistenceBackend.createTable(schemaName, null, null);
			} // if
			// creating the database and the table has only sense if working in row mode, in column node
			// everything must be provisioned in advance
			LOGGER.info(schemaName);
			LOGGER.info(Arrays.toString(queries.toArray()));
			persistenceBackend.insertContextData(schemaName, queries/*, fieldsNames, fieldsValues*/);
		} catch (Exception e) {
			throw new CygnusPersistenceError("-, " + e.getMessage());
		} // try catch
	} // persistAggregation

	/**
	 * Creates a PostgreSQL DB name given the FIWARE service.
	 * @param service
	 * @return The PostgreSQL DB name
	 * @throws CygnusBadConfiguration
	 */
	public String buildSchemaName(String service) throws CygnusBadConfiguration {
		String name;

		if (enableEncoding) {
			name = NGSICharsets.encodePostgreSQL(service);
		} else {
			name = NGSIUtils.encode(service, false, true);
		} // if else

		if (name.length() > NGSIConstants.MYSQL_MAX_NAME_LEN) {
			throw new CygnusBadConfiguration("Building schema name '" + name
					+ "' and its length is greater than " + NGSIConstants.MYSQL_MAX_NAME_LEN);
		} // if

		return name;
	} // buildSchemaName

} // NGSIPostgreSQLSink
