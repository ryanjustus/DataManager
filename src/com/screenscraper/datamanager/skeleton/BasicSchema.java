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
 * etc.
 *
 * @author Ryan
 */
public class BasicSchema implements RelationalSchema{


    private static Log log = LogFactory.getLog(DataManager.class);            
            
    /**
     * Maps column names to a nested map of properties or attributes about the
     * column.
     */
    protected Map<String,ObjectType> columnTypes;
    protected Map<String,Map<SchemaAttr,String>> columns;

    protected Set<SchemaKey> keys;
    protected Set<SchemaForeignKey> fks;
    protected SchemaKey primaryKey;
    private Map<String,DataObject.ValueParser> parsers;

    protected String name;
    private DataManagerEventSource eventSource;

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
     * @param attributes
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
     * @param attributeName
     * @param attributeValue
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
     * @param column
     * @param fkTableName
     * @param fkColumnName
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
     * @param column
     * @param fkTableName
     * @param fkColumnName
     */
    public RelationalSchema addForeignKey(String localColumn, String parentSchema, String parentColumn){
        localColumn = matchSchemaCase(localColumn);
        parentColumn = matchSchemaCase(parentColumn);
        Map<String,String> m = new HashMap<String,String>();
        m.put(localColumn, parentColumn);
        SchemaForeignKey fk = new SchemaForeignKey(localColumn, parentSchema, null);
        return addForeignKey(fk);
    }

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

    public boolean containsColumn(String column){
        Set<String> cols = getColumns();
        return containsIgnoreCase(column,cols);
    }

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

    public Set<String> getColumns()
    {
        return new HashSet(columns.keySet());
    }

    public Map<SchemaAttr,String> getAttrs(String column)
    {
      column = matchSchemaCase(column);
      return columns.get(column);
    }

    
    /**
     * THIS IS WRONG.  IT WILL SET THE DATE PARSER FOR ALL OBJECTS OF THAT TYPE
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
    
    
    public ObjectType getObjectType(String column){
        column = matchSchemaCase(column);
        ObjectType o = columnTypes.get(column);        
        return o;
    }

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

    public DataManagerEventListener addEventListener(EventFireTime when, DataManagerEventListener listener) {
        eventSource.addEventListener(when, listener);
        return listener;
    }

    public List<DataManagerEventListener> getEventListeners(EventFireTime when) {
        return eventSource.getEventListeners(when);
    }

    public RelationalSchema removeEventListener(EventFireTime when, DataManagerEventListener listener) {
        eventSource.removeEventListener(when, listener);
        return this;
    }

    public void fireEvent(EventFireTime when, DataManagerEvent evt) {
        eventSource.fireEvent(when, evt);
    }

    public Set<SchemaKey> getKeys() {
        return new HashSet(keys);
    }

    public Set<SchemaForeignKey> getForeignKeys() {
        return new HashSet<SchemaForeignKey>(fks);
    }

    public RelationalSchema setPrimaryKey(SchemaKey primary){
        if(primary.getType()!=SchemaKey.Type.primary){
            throw new IllegalArgumentException("Setting non primary key type as primary key");
        }
        primaryKey = primary;
        return this;
    }

    public RelationalSchema addIndex(SchemaKey key) {
        keys.add(key);
        return this;
    }

    public SchemaKey getPrimaryKey() {
        return primaryKey;
    }

    public Set<SchemaKey> getUniqueIndexes() {
        Set<SchemaKey> uniqueKeys = new HashSet<SchemaKey>();
        for(SchemaKey key : keys){
            if(key.getType()==SchemaKey.Type.unique){
                uniqueKeys.add(key);
            }
        }
        return uniqueKeys;
    }

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
    
    public void removeParser(String column){
        column = matchSchemaCase(column);
        parsers.remove(column);
    }

    public void setParser(String column, ValueParser parser) {
        column = matchSchemaCase(column);
        parsers.put(column, parser);
    }
    
    public ValueParser getParser(String column) {
        column = matchSchemaCase(column);
        return parsers.get(column);
    }
}