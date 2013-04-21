package com.screenscraper.datamanager.sql;

import com.screenscraper.datamanager.*;
import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.skeleton.*;
import com.screenscraper.datamanager.sql.SqlTableSchema.SqlSchemaAttr;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A DataManager specific to sql databases connected to with a jdbc driver via a BasicDataSource
 **/
public class SqlDataManager extends BasicDataManager implements DataManager
{
    private static Log log = LogFactory.getLog(DataManager.class);

    private BasicDataSource ds;
    private boolean threaded=false;
    private boolean autoManyToMany;
    private int numThreads=20;
    private int outstandingWrites;
    //keeps track of which schemas are set to be updateable and which are mergeable
  
    private ConcurrentHashMap<String, DataObject> lastAutoIncrementValues;
    ThreadPoolExecutor executor;

    /**     *
     * @param dataSource
     */
    public SqlDataManager(BasicDataSource dataSource)
    {
        super();
        outstandingWrites = 0;
        this.ds=dataSource;
        this.lastAutoIncrementValues = new ConcurrentHashMap<String,DataObject>();
        try {
            init();
        }
        catch(SQLException e){ 
            log.error(e.getMessage(),e);}
    }

    private void init() throws SQLException
    {
     
        autoManyToMany=false;
        currentDataNodes=new HashMap<String, DataNode>();
    }
    
    public int getNumOutstandingWrites(){
        return outstandingWrites;
    }
    
    /**
     * return a set of the columns which are primary keys defined in the SqlSchema
     * @param s
     * @return
     */
    public Set<String> getPrimaryKeys(String table)
    {
        SqlTableSchema s = (SqlTableSchema)schema.getRelationalSchema(table);
        if(s!=null){
            return s.getPrimaryKeyColumns();
        }
        return new HashSet();
        
    }
    
    /**
     * Add a foreign key to the schemas.  This is used if the underlying database engine
     * does not support built in foreign key constraints, or if for some reason the
     * tables do not use foreign key constraints but you want to relate data in the 
     * DataManager anyway
     * @param childTable
     * @param childColumn
     * @param parentTable
     * @param parentColumn
     * @return SchemaForeignKey that was generated and added to the child schema
     */
    public SchemaForeignKey addForeignKey(String childTable, String childColumn, String parentTable, String parentColumn){
        RelationalSchema child = this.getSchema(childTable);
        Map<String,String> fkMap = new HashMap<String,String>();
        fkMap.put(childColumn, parentColumn);
        SchemaForeignKey fk = new SchemaForeignKey("", parentTable, fkMap);
        child.addForeignKey(fk);
        return fk;
    }
    
    /**
     * Set the input date format for a date/time/timestamp column so that the database
     * can convert a String input it into an appropriate value for the database
     * @param table
     * @param column
     * @param inputDateFormat 
     */
    public void setDateFormat(String table, String column, SimpleDateFormat inputDateFormat){
        RelationalSchema schema = this.getSchema(table);
        ObjectType type = schema.getObjectType(column);
        if(type==ObjectType.Date || type==ObjectType.Time || type==ObjectType.Timestamp){
            schema.setDateFormat(column, inputDateFormat);
        }else{
            throw new IllegalArgumentException(table+"."+column+" is not a date/time/timestamp, cannot set date format");
        }
    }

    /**
     * returns the value of the autoincrement key for the table for the last commit, or null if none is set
     * @param table
     * @return
     */
    public DataObject getLastAutoIncrementValue(String table)
    {
        table = getSchema(table).getName();
        return lastAutoIncrementValues.get(table);
    }

    /**
     * Builds the schemas from a database.  This can take some time so if there
     * are any tables in the database that are not required for the DataManager
     * it is recommended to use buildSchemas(List<String> tables)
     * @throws Exception
     */
    @Override
    public void buildSchemas()
    {
        schema = new SqlSchemaBuilder(this).parseSchemasFromDb();
    }

     /**
     * Builds the schemas from a database including only tables
     * @throws Exception
     */
    public void buildSchemas(List<String> tables) throws Exception
    {
        schema = new SqlSchemaBuilder(this).parseSchemasFromDb(tables);
    }
    /*
     *
     */
    @Override
    protected synchronized DataWriter getNewDataWriter()
    {
        if(schema.getAttr(SqlDatabaseSchema.DatabaseSchemaAttr.Vendor).contains("HSQL")){
            return new HSqlDataWriter(this,lastAutoIncrementValues);
        }else{
            return new SqlDataWriter(this, lastAutoIncrementValues);
        }
    }



    /*
    Getters/Setters
     */

    /**
     * get a direct connection to the database from the Connection pool for specific queries
     * When the connection is done being used call close() on it to return it to the pool.
     * @return Sql Connection
     * @throws java.sql.SQLException
     */
    public synchronized Connection getConnection() throws SQLException
    {
        /*
        if(ds.getNumIdle()==0){
            log.warn("No free database connections");
        }
         * 
         */
        return ds.getConnection();
    }

        /**
     * Sets all the nodes to be able to overwrite columns in the database
     * with the new values when there is a primary key match.
     * @param update
     */
    public void setGlobalUpdateEnabled(boolean update)
    {
        if(update)
        {
            for(RelationalSchema s: schema.getRelationalSchemas())
            {
                updateable.add(s.getName());
            }
        }
        else
        {
            updateable=new HashSet();
        }
    }

    /**
     * Sets all the nodes to be able to merge the values with the corresponding
     * rows in the database when there is a primary key match.
     * @param merge
     */
    public void setGlobalMergeEnabled(boolean merge)
    {
        if(merge)
        {
            for(RelationalSchema s: schema.getRelationalSchemas())
            {
                mergeable.add(s.getName());
            }
        }
        else
        {
            mergeable=new HashSet();
        }

    }

      /**
     * verifies that the data is ready to be inserted.  If false is returned
     * the cause can be retrieved via getMessage.
     * @param schema
     * @return true if data meets constraints defined in the AbstractSchema
     */
    @Override
    public boolean checkDataNode(String schema)
    {
        RelationalSchema s = getSchema(schema);
        DataNode n = currentDataNodes.get(s.getName());
        if(n==null){
            log.debug("no existing DataNode for: "+schema);
            return false;
        }
        return validateData(n);
    }

       /**
     * toggles automatically generating many to many relationship nodes based
     * on schema information.  If a table has foreign keys to two or more tables,
     * those tables have been added, and all other constraints are met, the many
     * to many node will be generated and then written to the database when flush
     * is called
     * @param enable: default false
     */
    public void setAutoManyToMany(boolean enable)
    {
        this.autoManyToMany=enable;
    }


    /**
     * enables database writing to be done in its own thread.  numThreads is the maximum
     * number of threads that will be spawned.  numThreads cannot exceed 100.
     * @param numThreads
     */
    public synchronized void setMultiThreadWrite(int numThreads)
    {        
        if(numThreads<1)
            throw new IllegalArgumentException("numThreads must be greader than zero");
        ds.setMaxActive(numThreads+2);
        this.numThreads=numThreads+1;
        threaded=true;
    }

    private void submitWriterTask(final RootNode root)
    {
        if(executor==null)
        {
             executor= new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(1));
             executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        }
        executor.execute(new Runnable(){
                public void run() {
                    DataWriter dw = null;
                    try {
                        //log.info("flushing");
                        outstandingWrites++;
                        dw = getNewDataWriter();
                        dw.write(root);                        
                        return;
                    } catch (Exception ex) {
                        log.error(ex.getMessage(),ex);
                        return;
                    } finally{
                        outstandingWrites--;
                    }
                }
            });
    }

    /**
     * close should be called when you are done with the SqlDataManager
     */
    @Override
    public synchronized void close() {
        try {
            //hsql needs an explicit shutdown command
            if(schema.getAttr(SqlDatabaseSchema.DatabaseSchemaAttr.Vendor).contains("HSQL"))
            {
                Connection con=getConnection();
                Statement stmt = con.createStatement();
                stmt.execute("SHUTDOWN;");
                con.close();
            }
            ds.close();
            if(executor!=null)
                executor.shutdown();

        } catch (SQLException ex){
            log.error(ex.getMessage(),ex);
        }
    }



    /**
     * writes the current data to the Database and clears out all the data.
     * @return true if write is successful, if multithreaded writes are enabled
     * it always returns true as there is no callback function.
     */
    @Override
    public synchronized boolean flush(){
        
        RootNode node = this.root;
        clearAllData();
        if(threaded)
        {
            submitWriterTask(node);
            return true;
        }
        else
        {
            DataWriter dw;

            try {
                //log.info("flushing");
                dw = getNewDataWriter();
                dw.write(node);
            } catch (Exception ex) {
                log.error("error on flush: "  + ex.getMessage(),ex);
            }
            return true;
        }
    }

     /**
     * If a many to many relationship between this node and another in the set
     * of current datanodes is possible and doesn't already exist, that node
     * will be created.  This makes it so the end developer doesn't have to call
     * an empty addData to create the relationship.  If there are any other
     * constraints that aren't met the node won't be added because an addData would
     * be required anyway to complete the constraints.
     * @param n
     */
    protected void setManyToMany(DataNode n)
    {
        log.debug("setManyToMany: "+n.getSchema().getName());
        String table = n.getSchema().getName();
        for(RelationalSchema s: schema.getRelationalSchemas())
        {
            Set<String> fkTables = new HashSet();
            for(SchemaForeignKey fk : s.getForeignKeys()){
                fkTables.add(fk.getParentSchemaName());
            }
            String cTable = s.getName();
            //if the table is already in our current datanode list so skip it
            if(currentDataNodes.containsKey(cTable))
                continue;
            //if there is less than 2 fkColumns then it can't be a many to many
            if(fkTables.size()<2){
                continue;
            }
            if(fkTables.contains(table))
            {
                log.debug("MANY TO MANY "+s.getName()+"->"+table);
                log.debug("Existing datanodes: ");
                for(DataNode cn: this.currentDataNodes.values()){
                    log.debug(cn);
                }
                //check if each table has had data added, this makes it so we add
                //the many to many key last, after any other data could be added.
                boolean fkRelationshipNodesExist = true;
                for(String fkTable: fkTables){                   
                    if(!currentDataNodes.containsKey(fkTable) ){
                       fkRelationshipNodesExist = false;
                    }
                }
                if(fkRelationshipNodesExist){
                    DataNode manyToManyNode = this.getNewDataNode(cTable);
                    //check if all the other table constraints are met
                    if(validateData(manyToManyNode)) {
                        log.debug("Adding many to many relationship node " + cTable);
                        addData(manyToManyNode);
                        commit(cTable);
                    }                   
                }
            }
        }
    }

     /**
     * commits the row of data corresponding to the schema, so the next time addData is called
     * it will be in a new row.
     *
     * @param table
     * @return the value of checkDataNode called on the corresponding DataNode
     */
      /**
     * commits the row of data corresponding to the schema, so the next time addData is called
     * it will be in a new row.
     *
     * @param table
     * @return the value of checkDataNode called on the corresponding DataNode
     */
    @Override
    public boolean commit(String table)
    {
        
        RelationalSchema s = this.schema.getRelationalSchema(table);
        if(s==null){
            throw new IllegalArgumentException("unknown Schema " + schema);
        }        
        table = s.getName();
        DataNode n = getCurrentDataNode(table);
        if(n==null){
            n=getNewDataNode(table);
            this.currentDataNodes.put(table, n);
        }
        //fire the onCommit event
        schema.getRelationalSchema(table).fireEvent(EventFireTime.onCommit, new DataManagerEvent(this,n));        
        //boolean returnVal =  checkDataNode(table);
        n.setCommitted();
        connectTree(n);        
        if(autoManyToMany){
            setManyToMany(n);
        }
        
        currentDataNodes.remove(table);
        return true;
    }

    /**
     * This method is for when you have multi-threaded database writes enabled,
     * but you need certain writes to be inline, for example if you need to run
     * a query afterwards that depends on the data you just wrote being current.
     * @return true if write was successful, false otherwise.
     */
    public boolean flushSameThread()
    {
        RootNode node = this.root;
        clearAllData();
        DataWriter dw;

        try {
            //log.info("flushing");
            dw = getNewDataWriter();
            dw.write(node);
        } catch (Exception ex) {
            log.error("general error on flush", ex);
            return false;
        }
        return true;
    }
    
    public boolean validateData(DataNode n){
        
       RelationalSchema schema = n.getSchema();
       log.debug("Validating DataNode: " + n);
       // log.debug("VALIDATING NODE " + schema.getName());
        for(String column:schema.getColumns())
        {
            
            log.debug("checking column " + column);
            if(!keyConstraintsMet(n, column) && !checkNullValue(n,column))
            {
                return false;
            }
            if(!checkColumnLength(n,column))
            {
                return false;
            }
        }
        return true;
    }
    
    boolean keyConstraintsMet(DataNode n){
        SchemaKey pk = n.getSchema().getPrimaryKey();
        if(constraintMet(n,pk))
            return true;
        Set<SchemaKey> uniqueIndexes = n.getSchema().getUniqueIndexes();
        for(SchemaKey idx: uniqueIndexes){
            if(constraintMet(n,idx))
                return true;
        }
        return false;
    }
    
    boolean keyConstraintsMet(DataNode n, String column){
        SchemaKey pk = n.getSchema().getPrimaryKey();        
        if(constraintMet(n,pk))
            return true;
        Set<SchemaKey> uniqueIndexes = n.getSchema().getUniqueIndexes();
        for(SchemaKey idx: uniqueIndexes){
            if(constraintMet(n,idx))
                return true;
        }
        return false;
    }
    
    boolean constraintMet(DataNode n,SchemaKey k){
        if(k==null)
            return true;
        for(String column: k.getColumns()){
            if(n.getValue(column)==null){
                return false;
            }
        }
        return true;
       
    }
   
    boolean checkNullValue(DataNode n,String column)
    {
        RelationalSchema schema = n.getSchema();
        //check if the column value is set
        column = n.matchCase(column,schema);
        DataObject value = n.getValue(column);
        if(value==null || value.getObject()==null)
        {
            //check if column can have null values, if no nullable attribute is set
            //we assume that it can save null columns
            String nullable = schema.getAttr(column, BasicSchemaAttr.isNullable);
           // log.debug("...nullable " + nullable);
            if(nullable==null || nullable.equals(BasicSchemaAttr.Option.yes.toString()))
            {
                return true;
            }

            //check if there is a non-null default value
            String defaultValue = schema.getAttr(column, BasicSchemaAttr.defaultValue);
            //log.debug("...defaultValue " + defaultValue);
            if(defaultValue!=null && !defaultValue.equals("null"))
            {
                return true;
            }

            //check if it is an auto increment column
            String autoGen = schema.getAttr(column, BasicSchemaAttr.autoGenerated);
            if(autoGen!=null && autoGen.equals(BasicSchemaAttr.Option.yes.toString()))
            {
                return true;
            }
            
            //This is a hack to deal with the timestamp being auto-generated
            if(this.getDatabaseSchema().getAttr(SqlDatabaseSchema.DatabaseSchemaAttr.Vendor).equals("Microsoft SQL Server")){
                if(schema.getAttr(column, SqlSchemaAttr.sqlType).equals(String.valueOf(java.sql.Types.BINARY))){
                    return true;
                }
            }

            //check if it is a foreign key column (value will be set from parent)
            for(SchemaForeignKey fk : schema.getForeignKeys()){
                if(fk.getKeyMap().containsKey(column)){          
                        return true;
                    }                        
                }                  
            log.warn("Not Null Constraint for column "+column+" not met");
            return false;
        }
        else
        {
            return true;
        }
    }
    
    private DataNode getParent(DataNode child, RelationalSchema parentSchema){
        Set<DataNode> parents = new HashSet<DataNode>();
        for(DataNode parent: child.getParents()){
            if(parent.getSchema().getName().equals(parentSchema.getName())){
                return parent;
            }
        }
        return null;
    }
    
    public boolean fkConstraintsMet(DataNode n){
        log.debug("Validating foreign key constraints for : "+n);       
        Set<SchemaForeignKey> fks = n.getSchema().getForeignKeys();
        for(SchemaForeignKey fk: fks){
            DataNode parent = getParent(n,this.getSchema(fk.getParentSchemaName()));
 
            //IF the column isn't nullable, value isn't set
            //  AND parent is not committed or parent column is null
            //THEN return false            
            for(Entry<String,String> e: fk.getKeyMap().entrySet()){               
                if(
                  //Child value isn't set
                  (
                        n.getSchema().getAttr(e.getKey(), BasicSchemaAttr.isNullable)
                                   .equals(BasicSchemaAttr.Option.no.toString())
                        && n.getValue(e.getKey())==null
                   )
                   &&
                   //parent value also isn't set
                   (
                     !parent.isCommitted() 
                     || !checkNullValue(parent, e.getValue()) 
                   )
                 ){
                    return false;                    
                }
            }            
        }
        
        return true;        
    }

    boolean checkColumnLength(DataNode n,String column)
    {
        RelationalSchema schema = n.getSchema();
        ObjectType type = schema.getObjectType(column);
        if(!(type==ObjectType.String || type==ObjectType.ByteArray))
            return true;
        column = n.matchCase(column, schema);
        DataObject o = n.getValue(column);
        if(o==null)
            return true;
        Object value = o.getObject();
        if(value==null){
            return true;
        }
        Class c = o.getClass();
        String sizeString = schema.getAttr(column,BasicSchemaAttr.size);
        if(sizeString!=null)
        {
            int length = Integer.parseInt(sizeString);
            if(c.isArray())
            {
                if(length<=((Object[])value).length){
                    log.debug("Length constraint for column "+column+"not met");
                    return false;
                }
            }
            return (String.valueOf(o).length()<=length);
        }
        return true;
    }
}

