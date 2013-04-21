/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql;

import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.SchemaAttr;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.SchemaKey;
import com.screenscraper.datamanager.DatabaseSchema;
import com.screenscraper.datamanager.skeleton.*;
import com.screenscraper.datamanager.sql.SqlTableSchema.SqlSchemaAttr;
import com.screenscraper.datamanager.sql.util.JavaSql;
import com.screenscraper.datamanager.sql.util.QueryUtils;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 *
 * a class that parses the schemas from the database meta data.  This is called
 * from the SqlDataManager by the buildSchemas method.
 * @author ryan
 */
public class SqlSchemaBuilder
{
    private Log log = LogFactory.getLog(SqlSchemaBuilder.class);

    private SqlDataManager dm;

    /**
     * Generates the schemas from the database meta data
     * @param dm
     */
    public SqlSchemaBuilder(SqlDataManager dm)
    {
        this.dm=dm;
    }
    /**
     * returns constructs and returns the Schemas for the database
     * @return Schemas parsed from database
     */
    public DatabaseSchema parseSchemasFromDb()
    {
        DatabaseMetaData meta;
        List<String> tables = new ArrayList<String>();
        Connection conn=null;
        try {            
            conn=dm.getConnection();
            meta = conn.getMetaData();
            //get a list of all the tables of type TABLE (should exclude system and temporary tables)
            String[] tableTypes = {"TABLE"};
            ResultSet tableRS = meta.getTables(null, null, null, tableTypes);
            while(tableRS.next())
            {
                tables.add(tableRS.getString("TABLE_NAME"));
            }
            tableRS.close();            
        }
        catch(SQLException e)
        {
            log.error(e.getMessage(),e);
            return null;
        }finally{
            try {
                conn.close();
            } catch (SQLException ex) {}
        }
        return parseSchemasFromDb(tables);

    }
    
    public DatabaseSchema parseSchemasFromAccess(DatabaseSchema schemas, Connection con) throws SQLException{
        DatabaseMetaData md = con.getMetaData();
        ResultSet columnRS = md.getColumns(null, null, "Table1", null);
        while(columnRS.next()){
            ResultSetMetaData column_md = columnRS.getMetaData();
            int columns = column_md.getColumnCount();
            for(int i=1;i<=columns;i++){
                log.info("Column Data "+i+": "+column_md.getColumnName(i)+"::"+columnRS.getString(i));
            }
            
        }
        ResultSet indexRS = md.getIndexInfo(null, null, "Table1", false, true);
        
        while(indexRS.next()){
            ResultSetMetaData column_md = indexRS.getMetaData();
            int columns = column_md.getColumnCount();
            for(int i=1;i<=columns;i++){
                log.info("Index Data "+i+": "+column_md.getColumnName(i)+"::"+indexRS.getString(i));
            }
            
        }
        
        ResultSet fkRS = md.getCrossReference(null, null,"Table1", null, null, "Table2");
        while(fkRS.next()){
            String name = fkRS.getString("FK_NAME");
            name = (name==null) ? "" : name;

            //parent key column
            String pkColumn = fkRS.getString("PKCOLUMN_NAME");
            //key column in table
            String fkColumn = fkRS.getString("FKCOLUMN_NAME");
            log.info("PK Columns: "+pkColumn+"->"+fkColumn);
        }
        fkRS.close();
        
        return schemas;        
    }

    public DatabaseSchema parseSchemasFromDb(List<String> tables) {
        ArrayList<String> tablesLowerCase = new ArrayList();
        for(String table:tables){
            tablesLowerCase.add(table.toLowerCase());
        }
        DatabaseMetaData meta;
        Connection conn=null;
        try {            
            conn=dm.getConnection();
            meta = conn.getMetaData();
            DatabaseSchema schemas = this.parseDatabaseSchema(meta);
            
            //Alternative method for parsing schemas from microsoft access
            if(schemas.getAttr(SqlDatabaseSchema.DatabaseSchemaAttr.Vendor).toLowerCase().contains("access")){
                
                return this.parseSchemasFromAccess(schemas, conn);
            }
            
            //get a list of all the tables of type TABLE (should exclude system and temporary tables)
            String[] tableTypes = {"TABLE"};
            ResultSet tableRS = meta.getTables(null, null, null, tableTypes);
            
            while(tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");
                if(!tablesLowerCase.contains(tableName.toLowerCase()))
                    continue;
                Map<String,Map<SchemaAttr,String>> columns = new HashMap<String,Map<SchemaAttr,String>>();
                //first add in all the columns in the table with basic information;
                Map<String,ObjectType> columnTypes = new HashMap<String,ObjectType>();
                ResultSet columnRS = meta.getColumns(null, null, tableName, null);
                while(columnRS.next())
                {
                    Map<SchemaAttr,String> attributes = new HashMap<SchemaAttr, String>();
                    String column = columnRS.getString("COLUMN_NAME");
                    attributes.put(SqlSchemaAttr.columnName, column);
                    
                    attributes.put(SqlSchemaAttr.sqlType, columnRS.getString("DATA_TYPE"));
                    attributes.put(SqlSchemaAttr.sqlTypeName, columnRS.getString("TYPE_NAME"));
                    String nullable = columnRS.getString("NULLABLE").toLowerCase();
                    attributes.put(BasicSchemaAttr.isNullable, (nullable.equals("1")) ? BasicSchemaAttr.Option.yes.toString(): BasicSchemaAttr.Option.no.toString());
                    attributes.put(BasicSchemaAttr.defaultValue, columnRS.getString("COLUMN_DEF"));
                    attributes.put(BasicSchemaAttr.size, columnRS.getString("COLUMN_SIZE"));
                    
                    try{
                        attributes.put(BasicSchemaAttr.autoGenerated, columnRS.getString("IS_AUTOINCREMENT").toLowerCase());
                        if(attributes.get(BasicSchemaAttr.autoGenerated).equals(BasicSchemaAttr.Option.yes.toString())){
                            attributes.put(BasicSchemaAttr.autoGenerated, BasicSchemaAttr.Option.yes.toString());
                        }
                    }catch( SQLException e ){
                          //Not all databases (sqlite for example) have the IS_AUTOINCREMENT property here.
                        log.warn("AutoIncrement not supported for this database on table " + tableName);
                    }
           
                    columns.put(column , attributes);
                    ObjectType type=DataObject.ObjectType.Object;
                    try{
                        type = JavaSql.sqlToDataObjectTypeConvert(Integer.parseInt(attributes.get(SqlSchemaAttr.sqlType)));
                    }catch(Exception e){
                        log.warn("Error parsing type "+SqlSchemaAttr.sqlType+" for "+tableName+"."+column+". Defaulting to type 'Object'");                       
                    }
                    columnTypes.put(column, type);
                }
                columnRS.close();

                RelationalSchema s = new SqlTableSchema(tableName);
                for(Entry<String,Map<SchemaAttr,String>> column : columns.entrySet()){
                    s.addColumn(column.getKey(), columnTypes.get(column.getKey()), column.getValue());
                }
                schemas.addRelationalSchema(s);
                
            }
            tableRS.close();

             //now add in all the key information
            for(RelationalSchema s : schemas.getRelationalSchemas()){
                parsePrimaryKeys(s,meta);
                parseIndexKeys(s,meta);
                parseForeignKeys(s,schemas,meta);
            }            
            return schemas;
        }catch (SQLException ex) {
            log.error("Error while parsing schemas", ex);
        }finally{
            if(conn!=null)
                try{conn.close();}catch(SQLException e){}
        }
        return null;
    }

    private void parseIndexKeys(RelationalSchema s, DatabaseMetaData meta) throws SQLException{
        if(s==null)
            return;
        //add the primary key attribute to all matching columns
        Map<String,TreeMap<Integer,String>> keys = new HashMap<String,TreeMap<Integer,String>>();
        Map<String,Boolean> keyUnique = new HashMap<String,Boolean>();
        ResultSet keyRS = meta.getIndexInfo(null,null,s.getName(),false,true);
        while(keyRS.next()) {
            String name = keyRS.getString("INDEX_NAME");
            if(name==null){
                log.warn("INDEX NAME NOT FOUND "+ QueryUtils.saveRowAsMap(keyRS)+ ", SKIPPING");
                continue;
            }
            if(name.equals("PRIMARY")){
                continue;
            }
            TreeMap key = keys.get(name);
            if(key==null){
             key = new TreeMap<Integer,String>();
             keys.put(name, key);
            }
            int index = keyRS.getInt("ORDINAL_POSITION");
            boolean unique = !keyRS.getBoolean("NON_UNIQUE");
            keyUnique.put(name, unique);
            String column = keyRS.getString("COLUMN_NAME");
            key.put(index, column);
        }
        keyRS.close();

        //at this point we have all the columns saved into the keys Data Structure, now
        //we create a new Key with those values
        for(String name : keys.keySet()){
            List<String> columns = new ArrayList<String>();
            for(Entry<Integer,String> entry : keys.get(name).entrySet()){
                columns.add(entry.getValue());
            }
            boolean unique = keyUnique.get(name);
            SchemaKey key;
            if(unique){
                key = new SchemaKey(name, SchemaKey.Type.unique, columns);
            }else{
                key = new SchemaKey(name, SchemaKey.Type.normal, columns);
            }
            if(s.getPrimaryKey().equals(key))
                continue;
            s.addIndex(key);
        }
    }
    /**
     *
     * @param s
     * @param meta
     */
    private void parsePrimaryKeys(RelationalSchema s, DatabaseMetaData meta) throws SQLException{
        //add the primary key to the schema
        TreeMap<Integer,String> pk = new TreeMap<Integer,String>();
        ResultSet pkRS = meta.getPrimaryKeys(null, null, s.getName());
        String name = "";
        while(pkRS.next()) {
            name = pkRS.getString("PK_NAME");
            int index = pkRS.getInt("KEY_SEQ");
            String column = pkRS.getString("COLUMN_NAME");
            pk.put(index, column);
        }
        pkRS.close();

        List<String> k = new ArrayList<String>();
        for(Entry<Integer,String> entry: pk.entrySet()){
            k.add(entry.getValue());
        }
        if(!k.isEmpty()){
            SchemaKey key = new SchemaKey(name, SchemaKey.Type.primary, k);
            s.setPrimaryKey(key);
        }
        //There is no primary key defined, use all the columns as pk
        else{
            for(String col: s.getColumns()){
                k.add(col);
            }
            SchemaKey key = new SchemaKey(name, SchemaKey.Type.primary, k);
            s.setPrimaryKey(key);
        }
    }
    /**
     * Parses the foreign keys for the given schema from the database.  This method
     * is not used as the parseForeignKeys method below is much faster. See unit test
     * "ForeignKeyTest" for time comparison.
     * @param s
     * @param schemas
     * @param meta
     * @throws SQLException
     */
    public void parseForeignKeys2(RelationalSchema s, DatabaseSchema schemas, DatabaseMetaData meta) throws SQLException{
        
        String tableName = s.getName();
        for(RelationalSchema sChild : schemas.getRelationalSchemas()){
            String childTable = sChild.getName();
            //skip self table matches
            if(sChild.getName().equals(tableName))
                continue;
             //key name, order list of columns {column, parentColumn}
             Map<String, Map<String,String>> fks = new HashMap<String,Map<String,String>>();

             ResultSet fkRS = meta.getCrossReference(null, null, tableName, null, null,childTable);
             while(fkRS.next()){
                 String name = fkRS.getString("FK_NAME");
                 name = (name==null) ? "" : name;
                 Map key = fks.get(name);
                 if(key==null){
                     key = new HashMap<String,String>();
                     fks.put(name, key);
                 }
                 //parent key column
                 String pkColumn = fkRS.getString("PKCOLUMN_NAME");
                 //key column in table
                 String fkColumn = fkRS.getString("FKCOLUMN_NAME");
                 key.put(fkColumn,pkColumn);
             }
             fkRS.close();
             for(String name : fks.keySet()){
                 Map<String,String> keyMap = fks.get(name);
                 SchemaForeignKey fk = new SchemaForeignKey(name, tableName, keyMap);
                 sChild.addForeignKey(fk);
             }
        }
    }
    
    /**
     * Parses the foreign keys for the given schema from the database.
     * This looks up all the foreign Keys pointing to a tables pk.
     * @param s
     * @param schemas
     * @param meta
     * @throws SQLException
     */
    public void parseForeignKeys(RelationalSchema s, DatabaseSchema schemas, DatabaseMetaData meta) throws SQLException{
        
        String parentTable = s.getName();
        //KeyName -> (KeyMap->(ChildColumn->TableColumn)
        Map<String,ForeignKeyBuilder> fks = new HashMap<String,ForeignKeyBuilder>();
        ResultSet fkRS = meta.getExportedKeys(null, null, parentTable);
        while(fkRS.next()){
            String name = fkRS.getString("FK_NAME");
            String childTable = fkRS.getString("FKTABLE_NAME");
            name = (name==null) ? "" : name;
            ForeignKeyBuilder keyBuilder = fks.get(name);
            if(keyBuilder==null){
             keyBuilder = new ForeignKeyBuilder(name, childTable, parentTable);
             fks.put(name, keyBuilder);
            }
            //parent key column
            String pkColumn = fkRS.getString("PKCOLUMN_NAME");
            //key column in table
            String fkColumn = fkRS.getString("FKCOLUMN_NAME");
            keyBuilder.addColumnRelationship(fkColumn, pkColumn);            
        }
        for(ForeignKeyBuilder fk: fks.values()){
            String childTable = fk.childTable;            
            RelationalSchema childSchema = schemas.getRelationalSchema(childTable);
            childSchema.addForeignKey(fk.getForeignKey());
        }
        
    }

    private DatabaseSchema parseDatabaseSchema(DatabaseMetaData md) throws SQLException {   
        DatabaseSchema schema = new DatabaseSchema(); 
        log.info("MetaData: "+ md.toString());
        String driver = md.getDriverName().toUpperCase();
        if(driver.contains("JDBC-ODBC BRIDGE")){
            schema.addAttr(SqlDatabaseSchema.DatabaseSchemaAttr.MajorVersion, "unknown");
            schema.addAttr(SqlDatabaseSchema.DatabaseSchemaAttr.MinorVersion, "unknown");            
        }else{
            schema.addAttr(SqlDatabaseSchema.DatabaseSchemaAttr.MajorVersion, String.valueOf(md.getDatabaseMajorVersion()));
            schema.addAttr(SqlDatabaseSchema.DatabaseSchemaAttr.MinorVersion, String.valueOf(md.getDatabaseMinorVersion()));            
        }
        log.info("Vendor: "+md.getDatabaseProductName());
        schema.addAttr(SqlDatabaseSchema.DatabaseSchemaAttr.Vendor, md.getDatabaseProductName());
        schema.addAttr(SqlDatabaseSchema.DatabaseSchemaAttr.DatabaseIdentifierQuote, md.getIdentifierQuoteString());
        return schema;
    }
    
    class ForeignKeyBuilder{
        String keyName;
        String childTable;
        String parentTable;
        Map<String,String> keyMap;        
        public ForeignKeyBuilder(String keyName, String childTable, String parentTable){
            keyMap = new HashMap<String,String>();
            this.keyName=keyName;
            this.parentTable=parentTable;
            this.childTable=childTable;
        }
        
        public void addColumnRelationship(String childColumn, String parentColumn){
            keyMap.put(childColumn, parentColumn);
        }
        
        public SchemaForeignKey getForeignKey(){
            return new SchemaForeignKey(keyName, parentTable, keyMap);            
        }
    }
}