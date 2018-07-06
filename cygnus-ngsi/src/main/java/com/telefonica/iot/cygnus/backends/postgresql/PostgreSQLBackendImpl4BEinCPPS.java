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

package com.telefonica.iot.cygnus.backends.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import com.telefonica.iot.cygnus.errors.CygnusBadContextData;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.errors.CygnusRuntimeError;
import com.telefonica.iot.cygnus.log.CygnusLogger;

/**
 *
 * @author hermanjunge
 *
 * PostgreSQL related operations (database and table creation, context data insertion) when dealing with a PostgreSQL
 * persistence backend.
 */
public class PostgreSQLBackendImpl4BEinCPPS extends PostgreSQLBackendImpl /*implements PostgreSQLBackend*/ {

	protected PostgreSQLDriver driver;
	private static final CygnusLogger LOGGER = new CygnusLogger(PostgreSQLBackendImpl4BEinCPPS.class);
	protected PostgreSQLCache cache = null;

	/**
	 * Constructor.
	 * @param postgresqlHost
	 * @param postgresqlPort
	 * @param postgresqlDatabase
	 * @param postgresqlUsername
	 * @param postgresqlPassword
	 * @param enableCache
	 */
	public PostgreSQLBackendImpl4BEinCPPS(String postgresqlHost, String postgresqlPort, String postgresqlDatabase,
			String postgresqlUsername, String postgresqlPassword, boolean enableCache) {
		super(postgresqlHost, postgresqlPort, postgresqlDatabase, postgresqlUsername, postgresqlPassword, enableCache);
		if (enableCache) {
			cache = new PostgreSQLCache();
			LOGGER.info("PostgreSQL cache created succesfully");
		} // if

		driver = new PostgreSQLDriver(postgresqlHost, postgresqlPort, postgresqlDatabase, postgresqlUsername,
				postgresqlPassword);
	} // PostgreSQLBackendImpl4BEinCPPS

	/**
	 * Creates a table, given its name, if not exists in the given schema.
	 * @param schemaName
	 * @param tableName
	 * @param typedFieldNames
	 * @throws Exception
	 */
	@Override
	public void createTable(String schemaName, String tableName, String typedFieldNames) throws Exception {
		LOGGER.warn("The tables must exist. Create them and restart the service.");
	} // createTable

	public void insertContextData(String schemaName, /*String tableName[], String fieldNames[], String fieldValues[]*/
			List<String> queries)
					throws Exception {
		Statement stmt = null;

		// get a connection to the given database
		Connection con = driver.getConnection(schemaName);
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			throw new CygnusRuntimeError("Data insertion error", "SQLException", e.getMessage());
		} // try catch
		try {
			for (Iterator<String> iterator = queries.iterator(); iterator.hasNext();) {
				String query = (String) iterator.next();
				LOGGER.debug("Executing SQL query '" + query + "'");
				stmt.executeUpdate(query);
			}
		} catch (SQLTimeoutException e) {
			throw new CygnusPersistenceError("Data insertion error", "SQLTimeoutException", e.getMessage());
		} catch (SQLException e) {
			throw new CygnusBadContextData("Data insertion error", "SQLException", e.getMessage());
		} // try catch
	} // insertContextData

} // PostgreSQLBackendImpl
