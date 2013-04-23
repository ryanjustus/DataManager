/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.screenscraper.datamanager;

import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author ryan
 */
public interface DataManager {

	/**
	 * Builds the schemas from a database.  This can take some time so if there
	 * are any tables in the database that are not required for the DataManager
	 * it is recommended to use buildSchemas(List<String> tables)
	 */
	public void buildSchemas();

	/**
	 * Retrieve the columns for a given schema
	 * @param schema
	 * @return
	 */
	public Set<String> getColumns(String schema);

	/**
	 * returns a schema with the given identifier
	 * @param schema The name of the schema
	 * @return AbstractSchema corresponding the schema
	 */
	public RelationalSchema getSchema(String schema);

	/**
	 * returns whether or not th
	 * @param schema
	 * @return
	 */
	public boolean hasSchema(String schema);

	/**
	 * Add data to to the table represented by schema.  Entries corresponding to
	 * columns defined in the schema will be added.  addData can be called multiple
	 * times per row that you want to insert into the database, then when the row has all the
	 * data call commit(String schema) and the next time you call addData a new row will be started.
	 * Data is not actually written to the database until flush() is called.
	 * @param schema The name of the schema, or table
	 * @param data The data to add
	 */
	public void addData(String schema, Map<String, Object> data);

	/**
	 * Add data from the session variables to the table.
	 * Column name matches are case insensitive
	 * @param schema The name of the schema, or table
	 * @param columnName The name of the column in the database
	 * @param value The value to add
	 */
	public void addData(String schema, String columnName, Object value);

	/**
	 * commits the row of data corresponding to the schema, so the next time addData is called
	 * it will be in a new row.
	 *
	 * @param schema The name of the schema to commit
	 * @return the value of checkDataNode called on the corresponding DataNode
	 */
	public boolean commit(String schema);

	/**
	 * verifies that the data is ready to be inserted.  If false is returned
	 * the cause can be retrieved via getMessage.
	 * @param schema The name of the schema
	 * @return true if data meets constraints defined in the AbstractSchema
	 */
	public boolean checkDataNode(String schema);

	/**
	 * retrieves the current DataNode that is in scope for the schema
	 * @param schema The name of the schema
	 * @return current DataNode that is in scope for the schema
	 */
	public DataNode getCurrentDataNode(String schema);

	/**
	 * Returns a new data node from the provided schema identifier.  This should
	 * only be used if for some reason you need manual manipulation of the DataNode
	 * or DataNode tree. In general a DataManager.addData method should be used for
	 * adding data
	 * @param schema The name of the schema
	 * @return The new data node or null if the schema was invalid
	 */
	public DataNode getNewDataNode(String schema);

	/**
	 * Manually add a created DataNode to the DataManager for writing.  This can
	 * be used in conjunction with DataManager.getNewDataNode for instances when
	 * manual manipulation of a DataNode is required.  In general a DataManager.addData
	 * method should be used for adding data.
	 * @param n The node to add
	 */
	public void addData(DataNode n);

	/**
	 * Clear out all the uncommitted data for the given schema
	 * @param schema
	 */
	public void clearData(String schema);

	/**
	 * clears out all the nodes of the DataManager.  This is called by flush and
	 * generally wouldn't be requited by the user.  It is available for instances
	 * where when a certain condition is met you want to clear out all the information
	 * instead of writing.
	 */
	public void clearAllData();

	/**
	 * Add an even listener.  events are fired off during specific times
	 * @param schema
	 * @param when
	 * @param listener
	 * @return
	 */
	public DataManagerEventListener addEventListener(String schema, EventFireTime when, DataManagerEventListener listener);

	/**
	 * Manually set the DatabaseSchema. Use this for if you don't call buildSchemas
	 * for whatever reason
	 * @param s
	 * @return
	 */
	public DataManager setDatabaseSchema(DatabaseSchema s);

	/**
	 * Retrieve Database schema, for a Relational Database this contains
	 * all the information about the tables
	 * @return
	 */
	public DatabaseSchema getDatabaseSchema();

	/**
	 * writes the current data to the Database and clears out all the data.
	 * @return true if write is successful, if multithreaded writes are enabled
	 * it always returns true as there is no callback function.
	 */
	public boolean flush();

	/**
	 * returns the root node of the DataNode tree.  This can be handed off to a
	 * DataFilter to do preprocessing before the tree is written to the database.
	 * @return RootNode of the DataNode tree
	 */
	public RootNode getRoot();

	/**
	 * close should be called when you are done with the DataManager
	 */
	public void close();
}
