package com.screenscraper.datamanager.skeleton;
import com.screenscraper.datamanager.DataManager;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.SchemaKey;
import com.screenscraper.datamanager.DataManagerEvent;
import com.screenscraper.datamanager.DataManagerEventListener;
import com.screenscraper.datamanager.DataManagerEventSource;
import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.DataObject.ValueParser;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.SchemaAttr;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Object representing a unit of data storage, be it a database table, XML node,
 * etc.  This class should be ovewritten to provide specific functionality for
 * a given endpoint
 *
 * @author Ryan
 */
public class BasicSchema implements RelationalSchema{


    private static Log log = LogFactory.getLog(DataManager.class);            
            
    /**
     * Maps column names to a nested map of properties or attributes about the
     * column.
     */
    protected final Map<String,ObjectType> columnTypes;
    protected final Map<String,Map<SchemaAttr,String>> columns;

    protected final Set<SchemaKey> keys;
    protected final Set<SchemaForeignKey> fks;
    protected SchemaKey primaryKey;
    private final Map<String,DataObject.ValueParser> parsers;

    protected String name;
    private DataManagerEventSource eventSource;

	/**
	 * Constructs a RelationalSchema with the given name
	 * @param name
	 */
    public BasicSchema(String name){
        eventSource = new DataManagerEventSource();
        this.name=name;
        columnTypes = new HashMap<String,ObjectType>();
        columns = new HashMap<String,Map<SchemaAttr,String>>();
        keys = new HashSet<SchemaKey>();
        fks = new HashSet<SchemaForeignKey>();
        parsers = new HashMap<String,DataObject.ValueParser>();
    }



    /**
     * add a column to the AbstractSchema
     * @param column
     */
    public RelationalSchema addColumn(String column, ObjectType type)
    {
        column = matchSchemaCase(column);
        Map<SchemaAttr, String> dAttrs = getDefaultAttrs();
        Map<SchemaAttr, String> eAttrs = getAttrs(column);
        Map<SchemaAttr, String> nAttrs = new HashMap<SchemaAttr,String>();
        nAttrs.putAll(dAttrs);
        if(eAttrs!=null){
            nAttrs.putAll(eAttrs);
        }
        nAttrs.put(BasicSchemaAttr.type, type.name());
        columnTypes.put(column, type);
        columns.put(column, nAttrs);
        return this;
    }

    /**
     * add a column to the schema with the given attributes
     * @param column
     * @param attrs
     */
    public RelationalSchema addColumn(String column, ObjectType type, Map<SchemaAttr,String>attrs)
    {
        column = matchSchemaCase(column);
        addColumn(column, type);
        if(attrs!=null)
            columns.get(column).putAll(attrs);
        return this;
    }

    public Map<SchemaAttr, String> getDefaultAttrs(){
        Map<SchemaAttr, String> dAttr = new HashMap<SchemaAttr, String>();
        for(BasicSchemaAttr attr: BasicSchemaAttr.values()){
            String sAttr = attr.getDefaultOption();
            if(sAttr!=null)
                dAttr.put(attr, sAttr);
        }
        return dAttr;
    }

    public RelationalSchema addDefaultColumn(String column){
        column = matchSchemaCase(column);
        addColumn(column,DataObject.ObjectType.Object, getDefaultAttrs());
        return this;
    }

    public RelationalSchema setObjectType(String column, DataObject.ObjectType type){
        column = matchSchemaCase(column);
        if(!columns.keySet().contains(column)){
            throw new IllegalStateException("unknown column " + column + " for Schema " + getName());
        }
        addAttr(column, BasicSchemaAttr.type, type.toString());
        columnTypes.put(column, type);
        return this;
    }
    /**
     * Add an arbitrary attribute to the schema.
     * @param column
     * @param attr
     * @param attrVal
     */
    public RelationalSchema addAttr(String column, SchemaAttr attr, String attrVal) {
        if(column==null)  {
            throw new IllegalArgumentException("column is null");
        }else if(attr==null)  {
            throw new IllegalArgumentException("attr is null");
        }
        column = this.matchSchemaCase(column);

        Map<SchemaAttr,String> attributeMap = this.columns.get(column);
        if(attributeMap==null) {
            addDefaultColumn(column);
            attributeMap=columns.get(column);
        }
        attributeMap.put(attr, attrVal);
        return this;
    }

    /**
     * manually add the foreign key relationship, this would be used to link
     * schemas if the database engine doesn't support foreign keys.
     * @param fk
     */
    public RelationalSchema addForeignKey(SchemaForeignKey fk) {
        fks.add(fk);
        return this;
    }

     /**
     * manually add the foreign key relationship, this would be used to link
     * schemas if the database engine doesn't support foreign keys.  Using this
     * method only 1 column mapping can be defined for the key and the name of the
      * key is the same as the local column name
     * @param localColumn
     * @param parentSchema
     * @param parentColumn
     */
    public RelationalSchema addForeignKey(String localColumn, String parentSchema, String parentColumn){
        localColumn = matchSchemaCase(localColumn);
        parentColumn = matchSchemaCase(parentColumn);
        Map<String,String> m = new HashMap<String,String>();
        m.put(localColumn, parentColumn);
        SchemaForeignKey fk = new SchemaForeignKey(localColumn, parentSchema, null);
        return addForeignKey(fk);
    }

	/**
	 * Wraps all of the values in the input map as DataObjects, which contain additional type information
	 * concerning how to reconcile a given Java type with the database type
	 * @param data
	 * @return
	 */
    public Map<String,DataObject> convertToDataObject(Map<String,Object> data)
    {
        Map<String, DataObject> returnMap = new HashMap<String,DataObject>();
        for(String column: data.keySet())
        {
            if(!this.getColumns().contains(column))
            {
                throw new IllegalArgumentException("column " + column + " in whereData is invalid");
            }

            DataObject d = convertToDataObject(column, data.get(column));            
            returnMap.put(column,d);
        }
        return returnMap;
    }

   /**
   * returns all of the tables that the schema has foreign keys for
   * @return Set of all the foreign key tables this schema links to
   */
    public Set<String> getForeignTables()
    {
        HashSet<String> schemaFks = new HashSet<String>();
        for(Entry<String,Map<SchemaAttr,String>> e:columns.entrySet())
        {
            String fk=e.getValue().get(BasicSchemaAttr.parentSchema);
            if(fk!=null)
            {
                schemaFks.add(fk);
            }
        }
        return schemaFks;
    }

	/**
	 * Returns a Set of all columns where the database autogenerates the contents of a column, such as
	 * autoincrementing primary keys
	 * @return
	 */
    public Set<String> getAutoGeneratedColumns()
    {
        Set<String> c = new HashSet<String>();
        for(String column: this.getColumns())
        {
            String attribute = getAttr(column, BasicSchemaAttr.autoGenerated);
            if(BasicSchemaAttr.Option.yes.isEqual(attribute))
                c.add(column);
        }
        return c;
    }

	/**
	 * Returns whether or not the schema contains the given column, case insensitive
	 * @param column
	 * @return
	 */
    public boolean containsColumn(String column){
        Set<String> cols = getColumns();
        return containsIgnoreCase(column,cols);
    }

	/**
	 * returns whether or not the schema contains all the given columns, case insensitive
	 * @param columns
	 * @return
	 */
    public boolean containsAllColumns(Set<String> columns){
        for(String column: columns){
            if(!containsColumn(column))
                return false;
        }
        return true;
    }

    private static boolean containsIgnoreCase(String item, Collection<String> items){
        for(String i:items){
            if(item.equalsIgnoreCase(i))
                return true;
        }
        return false;
    }

	/**
	 * retrieve a list of all the columns for the schema
	 * @return
	 */
    public Set<String> getColumns()
    {
        return new HashSet(columns.keySet());
    }

	/**
	 * retrive a Map of all the attributes for a given column
	 * @param column
	 * @return
	 */
    public Map<SchemaAttr,String> getAttrs(String column)
    {
      column = matchSchemaCase(column);
      return columns.get(column);
    }

    
    /**
     * Returns a parser that converts a String in the given DateFormat into the target ObjectType
     * @param o
     * @param f 
     */
    private ValueParser getDateParser(ObjectType o, final DateFormat f){ 
        DataObject.ValueParser p;
        if(o==ObjectType.Timestamp){
            p = new ValueParser<java.sql.Timestamp>(){

                public java.sql.Timestamp parseFromString(String input) {
                   try {
                        Date d = f.parse(input);
                        return new java.sql.Timestamp(d.getTime());
                    } catch (ParseException ex) {
                        log.error(ex.getMessage(), ex);
                    }
                    return null;
                }
                

                public java.sql.Timestamp parseFromNumber(Number n) {
                    return new java.sql.Timestamp(n.longValue());
                }
            };
        }
        else if(o == ObjectType.Time)
        {
            p = new ValueParser<java.sql.Time>(){

                public java.sql.Time parseFromString(String input) {
                   try {
                        Date d = f.parse(input);
                        return new java.sql.Time(d.getTime());
                    } catch (ParseException ex) {
                        log.error(ex.getMessage(), ex);
                    }
                    return null;
                }

                public java.sql.Time parseFromNumber(Number n) {
                    return new java.sql.Time(n.longValue());
                }
            };
        }
        else if(o == ObjectType.Date)
        {
            p = new ValueParser<java.sql.Date>(){

                public java.sql.Date parseFromString(String input) {
                   try {
                        Date d = f.parse(input);
                        return new java.sql.Date(d.getTime());
                    } catch (ParseException ex) {
                        log.error(ex.getMessage(), ex);
                    }
                    return null;
                }

                public java.sql.Date parseFromNumber(Number n) {
                    return new java.sql.Date(n.longValue());
                }
            };
        }else{
            throw new IllegalStateException("can't add a date parser to a non date ObjectType " + o.getObjectClassName());
        }
        return p;
    }

	/**
	 * Returns the Object type for the given column.  ObjectType is used to reconcile the difference between Java
	 * types and jdbc types
	 * @param column
	 * @return
	 */
    public ObjectType getObjectType(String column){
        column = matchSchemaCase(column);
        ObjectType o = columnTypes.get(column);        
        return o;
    }

	/**
	 * Set the DateFormat for a given column for parsing dates in String format.
	 * The ObjectType of the column must be Date, Time, or Timestamp
	 * @param column
	 * @param f
	 */
    public void setDateFormat(String column, DateFormat f){
        column = matchSchemaCase(column);
        ObjectType type = columnTypes.get(column);
        if(type!=null && type==ObjectType.Date || type==ObjectType.Time || type==ObjectType.Timestamp){
            ValueParser p = getDateParser(type, f);
            setParser(column, p);
        }else{
            throw new IllegalArgumentException("Column type must be a date,time,or timestamp to set a date format, "+column+" type is "+type);
        }      
    }

	/**
	 * Retrives the value of a given SchemaAttr for the given column
	 * @param column
	 * @param attr
	 * @return
	 */
    public String getAttr(String column, SchemaAttr attr)
    {
        Map<SchemaAttr,String> columnMap=null;
        for(String key:columns.keySet())
        {
            if(key.toLowerCase().equals(column.toLowerCase()))
            {
                columnMap=columns.get(key);
                break;
            }
        }
        if(columnMap!=null)
        {
            return columnMap.get(attr);
        }
        else
            return null;
    }

     /**
     * returns a map with the default values for columns
     * @return default values for the schema
     */
    public Map<String, DataObject> getDefaultValues()
    {
        HashMap<String, DataObject> defaultValues = new HashMap<String, DataObject>();
        for(String column:columns.keySet())
        {
            String defaultValue=getAttr(column, BasicSchemaAttr.defaultValue);
            DataObject datum = convertToDataObject(column, defaultValue);
            defaultValues.put(column, datum);
        }
        return defaultValues;
    }

	/**
	 * Converts the given object into a DataObject for the specified column
	 * @param column
	 * @param o
	 * @return
	 */
    public DataObject convertToDataObject(String column, Object o)
    {
        column = matchSchemaCase(column);
        if(!containsColumn(column)){    
            log.debug("schema "+getName()+" doesn't contain column "+column + "...ignoring");
            return null;
        }
        if(o instanceof DataObject){
            return (DataObject)o;
        }
        log.warn("CONVERTING "+o+" TO DATAOBJECT");
        DataObject obj;
        if(parsers.containsKey(column)){
            ValueParser p = parsers.get(column);
            obj = new DataObject(o,this.getObjectType(column), p);
        }else{
            log.warn("NO USER DEFINED PARSER FOR "+column);
            log.info("COlUMN TYPE: "+this.getObjectType(column));
            obj = new DataObject(o, this.getObjectType(column));
        }        
        return obj;
    }
    /**
     * returns the name of the schema, for databases this corresponds to the
     * table name
     * @return table name
     */
    public String getName()
    {
        return name;
    }


    /**
    * Prints (effectively) debug information about the this AbstractSchema.
    * @return String representation of the schema
    */
    @Override
    public String toString() {
            StringBuilder value = new StringBuilder();

            value.append("Schema Identifier: ").append(this.name).append("\n")
                    .append("Table name: ").append(this.name).append("\n");
            
            value.append(primaryKey).append("\n");
            for(SchemaKey k: keys){
                value.append(k).append("\n");
            }
            for(SchemaForeignKey fk: fks){
                value.append(fk).append("\n");
            }
            Set<String> columnNames = columns.keySet();
            for (String columnName : columnNames)
            {
                    value.append("\tColumn name: ").append(columnName).append("\n")
                            .append("\t\t").append(columnTypes.get(columnName)).append("\n")
                            .append("\t\tAttributes:\n");

                    // Get attributes for this column
                    Map<SchemaAttr, String> attributes = columns.get(columnName);
                    Set<SchemaAttr> attributeNames = attributes.keySet();
                    for (SchemaAttr attributeName : attributeNames)
                    {
                            value.append("\t\t\t");
                            Object attributeValue = attributes.get(attributeName);
                            if (attributeValue instanceof BasicSchema)
                                    // Linked schema name (foreign key relationship)
                                    value.append(attributeName).append(" : <").append(((BasicSchema)attributeValue).name);
                            else
                                    // Actual value, if not a AbstractSchema
                                    value.append(attributeName).append(" : ").append(attributeValue);
                            value.append("\n");
                    }
            }
            return value.toString();
    }

	/**
	 * Add an event listener to the schema
	 * @param when
	 * @param listener
	 * @return
	 */
    public DataManagerEventListener addEventListener(EventFireTime when, DataManagerEventListener listener) {
        eventSource.addEventListener(when, listener);
        return listener;
    }

	/**
	 * Retrieve the event listeners for a give EventFireTime
	 * @param when
	 * @return
	 */
    public List<DataManagerEventListener> getEventListeners(EventFireTime when) {
        return eventSource.getEventListeners(when);
    }

	/**
	 * Removes the event listener
	 * @param when
	 * @param listener
	 * @return
	 */
    public RelationalSchema removeEventListener(EventFireTime when, DataManagerEventListener listener) {
        eventSource.removeEventListener(when, listener);
        return this;
    }

	/**
	 * Fire off the event
	 * @param when
	 * @param evt
	 */
    public void fireEvent(EventFireTime when, DataManagerEvent evt) {
        eventSource.fireEvent(when, evt);
    }

	/**
	 * Retrieves the table keys for the schema
	 * @return
	 */
    public Set<SchemaKey> getKeys() {
        return new HashSet(keys);
    }

	/**
	 * Retrieve all teh foreign keys for the schema
	 * @return
	 */
    public Set<SchemaForeignKey> getForeignKeys() {
        return new HashSet<SchemaForeignKey>(fks);
    }

	/**
	 * Set the pk for the schema
	 * @param primary
	 * @return
	 */
    public RelationalSchema setPrimaryKey(SchemaKey primary){
        if(primary.getType()!=SchemaKey.Type.primary){
            throw new IllegalArgumentException("Setting non primary key type as primary key");
        }
        primaryKey = primary;
        return this;
    }

	/**
	 * Add an index to the schema
	 * @param key
	 * @return
	 */
    public RelationalSchema addIndex(SchemaKey key) {
        keys.add(key);
        return this;
    }

	/**
	 * Retrieve the pk for the schema
	 * @return
	 */
    public SchemaKey getPrimaryKey() {
        return primaryKey;
    }

	/**
	 * retrieve all the unique indexes for the schema
	 * @return
	 */
    public Set<SchemaKey> getUniqueIndexes() {
        Set<SchemaKey> uniqueKeys = new HashSet<SchemaKey>();
        for(SchemaKey key : keys){
            if(key.getType()==SchemaKey.Type.unique){
                uniqueKeys.add(key);
            }
        }
        return uniqueKeys;
    }

	/**
	 * retrieve the foreign keys corresponding to a given parent table
	 * @param parentTable
	 * @return
	 */
    public Set<SchemaForeignKey> getForeignKeys(String parentTable) {
        Set<SchemaForeignKey> retKey = new HashSet<SchemaForeignKey>();
        for(SchemaForeignKey fk: fks){
            if(fk.getParentSchemaName().equalsIgnoreCase(parentTable)){
                retKey.add(fk);
            }
        }
        return retKey;
    }


    private String matchSchemaCase(String inputCase)
    {
        for(String schemaCase:getColumns())
        {
            if(inputCase.equalsIgnoreCase(schemaCase)){
                return schemaCase;
            }
        }
        return inputCase;
    }

	/**
	 * remove the ValueParser for a column
	 * @param column
	 */
    public void removeParser(String column){
        column = matchSchemaCase(column);
        parsers.remove(column);
    }

	/**
	 * Set the ValueParser for a column, value parsers are used to convert from a String or Number to the given
	 * database target type
	 * @param column
	 * @param parser
	 */
    public void setParser(String column, ValueParser parser) {
        column = matchSchemaCase(column);
        parsers.put(column, parser);
    }

	/**
	 * Retrieve the ValueParser for a given column
	 * @param column
	 * @return
	 */
    public ValueParser getParser(String column) {
        column = matchSchemaCase(column);
        return parsers.get(column);
    }
}