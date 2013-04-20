/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import com.screenscraper.datamanager.DataObject.ObjectType;
import java.text.DateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * General methods available to a Schema.  Most of these are implemented in the
 *
 * @author ryan
 */
public interface RelationalSchema {

    /**
     * Returns the ObjectType corresponding to the column.  These are java
     * type names as opposed to database type names.
     * @param column
     * @return
     */
    public ObjectType getObjectType(String column);
    /**
     * Sets the object type of the column.
     * @param column
     * @param type
     */
    public RelationalSchema setObjectType(String column, DataObject.ObjectType type);
    /**
     * retrieve an attribute value from the schema
     * @param column
     * @param attr
     * @return
     */
    public String getAttr(String column, SchemaAttr attr);
    /**
     * retrieve all the attribute values for a specific column
     * @param column
     * @return
     */
    public Map<SchemaAttr,String> getAttrs(String column);
    /**
     * add an attribute to a column
     * @param column
     * @param attr
     * @param attrValue
     */
    public RelationalSchema addAttr(String column, SchemaAttr attr, String attrValue);
    /**
     * retrieve all the columns in the Schema
     * @return
     */
    public Set<String> getColumns();
    /**
     * add a column to the DataManager
     * @param column
     * @param type
     */
    public RelationalSchema addColumn(String column, ObjectType type);
    /**
     * add a column with the attributes to the schema
     * @param column
     * @param type
     * @param attrs
     */
    public RelationalSchema addColumn(String column, ObjectType type, Map<SchemaAttr,String> attrs);
    /**
     * convert an Object to a DataObject.  A DataObject contains additional information
     * such as type information and also contains information of how to parse values
     * from Strings and Numbers into the Object
     * @param column
     * @param o
     * @return
     */
    public DataObject convertToDataObject(String column, Object o);
    /**
     * retrieves the name of the Schema
     * @return
     */
    public String getName();
    /**
     * Set a column as a key column in the schema
     * @param column
     */
    public RelationalSchema addIndex(SchemaKey key);

    /**
     * Set a foreign key in the schema
     * @param column
     */
    public RelationalSchema addForeignKey(SchemaForeignKey fk);
    /**
     * retrieve a map with the default values of the columns
     * @return
     */
    public Map<String,DataObject> getDefaultValues();

    /**
     * retrieve the columns whose values are Auto-generated (don't need to be added
     * via the DataManager.addData method)
     * @return
     */
    public Set<String> getAutoGeneratedColumns();
    /**
     * retrieve the foreign keys associated with this schema.
     * @return [column, fkTable, fkColumn]
     */
    public Set<SchemaForeignKey> getForeignKeys();


    /**
     * retrieve the primary key associated with this schema.
     * @return [column, fkTable, fkColumn]
     */
    public SchemaKey getPrimaryKey();

    /**
     * retrieve the non-primary key unique index's associated with the schema
     * @return [column, fkTable, fkColumn]
     */
    public Set<SchemaKey> getUniqueIndexes();



    /*
     * Retrieve the column that points to the Foreign table
     * @param table
     * @return
     */

    public Set<SchemaForeignKey> getForeignKeys(String parentTable);
    /**
     * returns whether the Schema contains the specified column, case insensitive
     * @param column
     * @return
     */
    public boolean containsColumn(String column);
    /**
     * returns whether the Schema contains the specified column, case insensitive
     * @param columns
     * @return
     */
    public boolean containsAllColumns(Set<String> columns);
    public DataManagerEventListener addEventListener(EventFireTime when, DataManagerEventListener listener);
    public List<DataManagerEventListener> getEventListeners(EventFireTime when);
    public RelationalSchema removeEventListener(EventFireTime when, DataManagerEventListener listener);
    public void fireEvent(EventFireTime when, DataManagerEvent evt);
    public RelationalSchema setPrimaryKey(SchemaKey primary);
    public void setDateFormat(String column, DateFormat f);
    public void setParser(String column, DataObject.ValueParser parser);
}