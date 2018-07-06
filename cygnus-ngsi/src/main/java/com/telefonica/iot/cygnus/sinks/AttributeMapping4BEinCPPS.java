/**
 * 
 */
package com.telefonica.iot.cygnus.sinks;

import com.telefonica.iot.cygnus.log.CygnusLogger;

/**
 * @author gianluca
 *
 */
public class AttributeMapping4BEinCPPS {

	private String tableName;
	private String columnName;
	//private String dataType;
	private String value;
	
	private static final CygnusLogger LOGGER = new CygnusLogger(AttributeMapping4BEinCPPS.class);
	
	public AttributeMapping4BEinCPPS(String value) {
		super();
		
		String[] s = value.split("\\.");
		if(s.length != 2) {
			LOGGER.error("Error while get mapping for a CB Attribute...\n The rigth structure is <table_name>.<column_name>.<data_type>");
		}
		setTableName(s[0]);
		setColumnName(s[1]);
		//setDataType(s[2]);
	}

	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
		
}
