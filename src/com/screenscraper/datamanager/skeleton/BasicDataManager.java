/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.screenscraper.datamanager.skeleton;

import com.screenscraper.datamanager.DataAssertion;
import com.screenscraper.datamanager.DataManager;
import com.screenscraper.datamanager.DataManagerEvent;
import com.screenscraper.datamanager.DataManagerEventListener;
import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataWriter;
import com.screenscraper.datamanager.DatabaseSchema;
import com.screenscraper.datamanager.RootNode;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.util.LogAppender;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Map.Entry;




/**
 * A DataManager contains the tree of DataNodes and also provides useful methods 
 * for adding data, writing, interacting with Schemas, etc.
 *
 * The DataManager
 * is the primary class for inserting, manipulating, and writing data to the
 * underlying DataSource.
 *
 * BasicDataManager implements many of the re-usable features of a DataManager, but
 * is not intended to be an implemented class
 *
 * @author ryan
 */
public abstract class BasicDataManager implements DataManager{

    //the root node of the DataManager, all nodes you add will be an ancestor of this
    //Mutable data
    protected RootNode root;
    protected Map<String, DataNode> currentDataNodes;

     //the available schemas for the DataManager
    protected DatabaseSchema schema;
    protected Set<String> updateable;
    protected Set<String> mergeable; 
    
    private final LogAppender logAppender;    
    private static Log log = LogFactory.getLog(DataManager.class);

    /**
     * Creates a DataManager tied to a screen-scraper session
     */
    public BasicDataManager()
    {
        logAppender= new LogAppender();
        init();        
    }

    private void init()
    {
        updateable = new HashSet<String>();
        mergeable = new HashSet<String>();
        schema = new DatabaseSchema();
        root = new RootNode();
        currentDataNodes=new HashMap<String,DataNode>();
    }

	/**
	 * Add an appender to receive log events from the DataManager
	 * @param log
	 */
    public void appendToLogger(Logger log){
        logAppender.addLogger(log);
    }


	/**
	 * Factory method to construct a DataWriter for the endpoint
	 * @return
	 */
    protected DataWriter getNewDataWriter(){
        throw new UnsupportedOperationException("Must be overriden by implementing class");
    }


	/**
	 * Retrieves the set of columns for the given schema name
 	 * @param schema
	 * @return
	 */
    public Set<String> getColumns(String schema)
    {
        return this.schema.getRelationalSchema(schema).getColumns();
    }


    /**
     * returns a schema with the given identifier (case insensitive)
     * @param schema
     * @return RelationalSchema corresponding the schema name
     */
    public RelationalSchema getSchema(String schema) {
       for(RelationalSchema s: this.schema.getRelationalSchemas())
       {
           if(s.getName().toLowerCase().equals(schema.toLowerCase()))
               return s;
       }
       throw new IllegalArgumentException("Unknown schema " + schema);
    }

	/**
	 * Returns whether or not the given schema is known to the DataManager
	 * @param schema
	 * @return
	 */
    public boolean hasSchema(String schema){
       for(RelationalSchema s: this.schema.getRelationalSchemas())
       {
           if(s.getName().toLowerCase().equals(schema.toLowerCase()))
               return true;
       }
       return false;        
    }

     /**
     * Sets all the nodes to be able to overwrite columns in the table
     * with the new values when there is a primary key match.
     * @param schema
     * @param update: default false
     */
    public void setUpdateEnabled(String schema, boolean update)
    {
        schema = getSchema(schema).getName();
        if(update){
            updateable.add(schema);
        }else
            updateable.remove(schema);
    }

    /**
     * Sets all the nodes to be able to merge the values with the corresponding
     * rows in the schema when there is a primary key match.
     * @param schema
     * @param merge: default false
     */
    public void setMergeEnabled(String schema, boolean merge)
    {
        schema = getSchema(schema).getName();
        if(merge){
            mergeable.add(schema);
        }else
            mergeable.remove(schema);
    }

     /**
     * Add data to to the table represented by schema.  Entries corresponding to
     * columns defined in the schema will be added.  addData can be called multiple
     * times per row that you want to insert into the database, then when the row has all the
     * data call commit(String schema) and the next time you call addData a new row will be started.
     * Data is not actually written to the database until flush() is called.
     * @param schema
     * @param data
     */
    public void addData(String schema, Map<String,Object>data)
    {
        schema = this.schema.getRelationalSchema(schema).getName();
        DataNode n;
        if(currentDataNodes.containsKey(schema))
        {
            n = currentDataNodes.get(schema);
            try{
            n.addData(data);
            }catch(UnsupportedEncodingException e){
                log.error("Error encoding data for schema "+schema, e);
                return;
            }
        }
        else
        {
            if(this.schema==null)
                throw new IllegalStateException("no schemas defined");
            if(getSchema(schema)==null)
                throw new IllegalArgumentException("unknown schema " + schema);
            n = getNewDataNode(schema);
            try{
            n.addData(data);
            }catch(UnsupportedEncodingException e){
                log.error("Error encoding data for schema "+schema, e);
                return;
            }
            currentDataNodes.put(schema, n);
        }
        if(mergeable.contains(n.getSchema().getName())){
            n.setMergeable();
        }
        if(updateable.contains(n.getSchema().getName())){
            n.setUpdateable();
        }
        //run user added event listeners
        n.getSchema().fireEvent(EventFireTime.onAddData, new DataManagerEvent(this,n));
    }


    /**
     * Add data from the session variables to the table.
     * Column name matches are case insensitive
     * @param schema
     * @throws java.io.UnsupportedEncodingException
     */
    public void addData(String schema, String columnName, Object value)
    {
        HashMap<String, Object>m = new HashMap();
        m.put(columnName, value);
        addData(schema, m);
    }
    

    /**
     * commits the row of data corresponding to the schema(table), so the next time addData is called
     * it will be in a new row.
     *
     * @param schema, name of schema you are committing
     * @return the value of checkDataNode called on the corresponding DataNode
     */
    public boolean commit(String schema)
    {
        RelationalSchema s = getSchema(schema);       
        boolean returnVal =  checkDataNode(s.getName());
        DataNode n = getCurrentDataNode(s.getName());
        n.setCommitted();
        connectTree(n);        
        currentDataNodes.remove(s.getName());
        //fire user events
        s.fireEvent(EventFireTime.onCommit, new DataManagerEvent(this,n));
        return returnVal;
    }

    /**
     * verifies that the data is ready to be inserted.  If false is returned
     * the cause can be retrieved via getMessage.
     * @param schema
     * @return true if data meets constraints defined in the AbstractSchema
     */
    public boolean checkDataNode(String schema){
        return true;
    }

    /**
     * retrieves the current DataNode that is in scope for the schema
     * @param schema
     * @return current DataNode that is in scope for the schema
     */
    public DataNode getCurrentDataNode(String schema)
    {
        RelationalSchema s = this.schema.getRelationalSchema(schema);
        return currentDataNodes.get(s.getName());
    }

    /**
     * removes the DataNode corresponding to the schema prior ro flush being called
     * @param schema
     */
    public void rollback(String schema)
    {
        DataNode nodeToRemove=currentDataNodes.get(schema);
        currentDataNodes.remove(schema);
        root.removeChild(nodeToRemove);
    }


    private void removeDataNode(DataNode testNode, DataNode n, Set<DataNode> visitedNodes)
    {
        visitedNodes.add(testNode);
        Iterator<DataNode> itr = testNode.getChildren().iterator();
        while(itr.hasNext())
        {
            DataNode child = itr.next();
            if(child==n)
                itr.remove();
            else if(!visitedNodes.contains(child))
            {
                removeDataNode(child, n, visitedNodes);
            }
        }
    }

    /**
     * @param n
     * @return true if none of the foreign key values are null
     */
    private boolean foreignKeysSet(DataNode n)
    {
        for(SchemaForeignKey fk : n.getSchema().getForeignKeys()){
            for(String column : fk.getKeyMap().keySet()){
                log.debug("checking if foreign key column " +column+" is set...");
                if(n.getValue(column)==null || n.getValue(column).getObject()==null){
                    return false;
                }
            }
        }
        return true;
    }
    
    /**
     * @param n
     * @return true if none of the foreign key values are null
     */
    private boolean foreignKeySet(DataNode n, String parent)
    {
        for(SchemaForeignKey fk : n.getSchema().getForeignKeys(parent)){
            for(String column : fk.getKeyMap().keySet()){
                log.debug("checking if foreign key column " +column+" is set...");
                if(n.getValue(column)==null || n.getValue(column).getObject()==null){
                    return false;
                }
            }
        }
        return true;
    }

 

    /**
     * Commit calls this method to create necessary parent nodes and connect
     * this node to the active DataNode tree
     * @param n
     */
    protected void connectTree(DataNode n)    {  
        for(SchemaForeignKey fk: n.getSchema().getForeignKeys()){            
            String table = fk.getParentSchemaName();            
            //Always link it to a parent if one exists, this is just to control the order of the writes
            if(currentDataNodes.containsKey(table)){
                log.debug("linking DataNode "+n.getSchema().getName()+" to parent "+table + " for ForeignKey "+fk);
                DataNode parent = currentDataNodes.get(table);
                parent.addChild(n);
            }
            //If a foreign key constraint fails then create an empty parent placeholder
            else if(!foreignKeySet(n,table)){
                DataNode parent = this.getNewDataNode(table);
                this.addData(parent);
                parent.addChild(n);
            }            
        }
        //If the node doesn't have any parents then add it to the root node
        if(n.getParents().isEmpty()){
            root.addChild(n);
        }
    }
    /**
     * Returns a new data node from the provided schema identifier.  This should
     * only be used if for some reason you need manual manipulation of the DataNode
     * or DataNode tree. In general a DataManager.addData method should be used for
     * adding data
     * @param schema
     * @return The new data node or null if the schema was invalid
     */
    public DataNode getNewDataNode( String schema )
    {
        RelationalSchema s = getSchema(schema);
        DataNode n = new DataNode(s);
        s.fireEvent(EventFireTime.onCreate, new DataManagerEvent(this,n));
        return n;        
    }


    /**
     * Manually add a created DataNode to the DataManager for writing.  This can
     * be used in conjunction with DataManager.getNewDataNode for instances when
     * manual manipulation of a DataNode is required.  In general a DataManager.addData
     * method should be used for adding data.
     * @param n
     */
    public void addData(DataNode n)
    {
        String name = n.getSchema().getName();            
        currentDataNodes.put(name, n);
        root.addChild(n);
    }

   

    /**
     * clears out all the nodes of the DataManager.  This is called by flush and
     * generally wouldn't be requited by the user.  It is available for instances
     * where when a certain condition is met you want to clear out all the information
     * instead of writing.
     */
    public synchronized void clearAllData() {
            root = new RootNode();
            this.currentDataNodes.clear();
    }

    /**
     * 
     * @return true if flush was successful
     */
    public boolean flush(){
        DataWriter dw = getNewDataWriter();
        boolean returnval = dw.write(root);
        this.clearAllData();
        return returnval;
    }

    /**
     * returns the root node of the DataNode tree.
     * In most cases there is no need to ever manipulate the tree structure with the data
     * @return RootNode of the DataNode tree
     */
    public RootNode getRoot()
    {
        return root;
    }

     /**
     * generates a 40 character UID comprised of 0-9a-z based on a SHA1 hash
     * @param name
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String generateUID(String name) throws NoSuchAlgorithmException
    {
        String seed = Math.random()*1000 + name +System.nanoTime();
        MessageDigest m = MessageDigest.getInstance("SHA1");
        m.update(seed.getBytes(),0,seed.length());
        return new BigInteger(1,m.digest()).toString(36);
    }

    /**
     * Hashes the return of catData(m) with a SHA1 hash
     * @param m
     * @return 31 character hash comprised of 0-9a-z based on a SHA1 hash
     */
    public static String hashData(SortedMap<String,DataObject> m)
    {
        String ret = "";
        try
        {
            String seed = catData(m);
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(seed.getBytes(),0,seed.length());
            ret = new BigInteger(1,md.digest()).toString(36);
            int zeros = 31-ret.length();
            if(zeros>0){
                String leadingZeros = "";
                for(int i=0;i<zeros;i++){
                    leadingZeros+="0";
                }
                ret=leadingZeros+ret;
            }
        }catch(NoSuchAlgorithmException e){
            log.error("error hashing data", e);
        }
        return ret;
    }

    /**
     * hashes the return of catData(n,columns) with a SHA1 hash
     * @param n
     * @param columns
     * @return 40 character hash comprised of 0-9a-z based on a SHA1 hash
     */
    public static String hashData(DataNode n, Set<String> columns)
    {
        if(n==null){
            return "";
        }
        if(columns==null){
            columns.addAll(n.getSchema().getColumns());
        }
        SortedMap<String,DataObject> m = new TreeMap<String,DataObject>();
        for(Entry<String,DataObject> entry: n.getValues().entrySet()){
            String key = entry.getKey();
            DataObject value = entry.getValue();
            if(columns.contains(key)){
                m.put(key,value);
            }
        }
        return hashData(m);
    }

    /**
     * Concatenates the key value pairs of the map
     * @param m Sorted map to generate a cat String from.
     * @return concatenated key-value string of Map
     */
    public static String catData(SortedMap<String,DataObject> m){
        if(m==null)
            return "";
        StringBuilder s = new StringBuilder();
        for(Entry<String,DataObject> e: m.entrySet()){
            s.append(e.getKey()).append(e.getValue().getObject());
        }
        return s.toString();
    }

    /**
     * Concatenates the key value pairs of the specified columns in the DataNode
     * @param n
     * @param columns
     * @return concatenated key-value string of Map
     */
    public static String catData(DataNode n, Set<String> columns){
        if(n==null)
            return "";

        if(columns==null){
            columns.addAll(n.getSchema().getColumns());
        }
        SortedMap<String,DataObject> m = new TreeMap<String,DataObject>();
        for(Entry<String,DataObject> entry: n.getValues().entrySet()){
            String key = entry.getKey();
            DataObject value = entry.getValue();
            if(columns.contains(key)){
                m.put(key,value);
            }
        }
        return catData(m);
    }


    public void close(){

    }


	/**
	 * Removes uncommitted data for the given schema name
	 * @param schema
	 */
    public void clearData(String schema) {
        this.currentDataNodes.remove(schema);
    }

	/**
	 * Set the database schema.  Normally buildSchemas is called to initialize the schemas, but it is possible
	 * to manually build or import the schemas instead
	 * @param s
	 * @return
	 */
    public DataManager setDatabaseSchema(DatabaseSchema s) {
        schema = s;
        return this;
    }

	/**
	 * Returns the current DatabaseSchema the DataManager instance is using.  A DatabaseSchema corresponds to
	 * a description of all the tables in a relational database
	 * @return
	 */
    public DatabaseSchema getDatabaseSchema() {
        return schema;
    }
    
    /**
     * Adds a data assertion to be performed when the write is called.
     * A DataAssertion is a special case of a DataManagerEvent that executes
     * onSuccess if the assertion passes and onFail if the assertion fails
     * @param d
     * @return 
     */
    public DataManagerEventListener addDataAssertion(final DataAssertion d){
        return addDataAssertion(d,EventFireTime.onWrite);       
    }

    /**
     * Adds a DataAssertion to be run
     * A DataAssertion is a special case of a DataManagerEvent that executes
     * onSuccess if the assertion passes and onFail if the assertion fails.
     * It is one way to filter out garbage data before it is written
     * @param d
     * @param when
     * @return 
     */
    public DataManagerEventListener addDataAssertion(final DataAssertion d, EventFireTime when) {
        DataManagerEventListener l = new DataManagerEventListener(){
            public void handleEvent(DataManagerEvent event) {
                if(d.validateData(event.getDataNode())){
                    d.onSuccess();
                }else{
                    d.onFail();
                }
            }
        };
        return addEventListener(d.getSchema(), when, l);
    }
    
    /**
     * Adds an event listener.  Event listeners are fired at specific times and can be used to perform callbacks
     * when certain actions occur, such as a failure to insert a given DataNode
     * @param schema
     * @param when
     * @param listener
     * @return 
     */
     public DataManagerEventListener addEventListener(String schema, EventFireTime when, DataManagerEventListener listener){
        RelationalSchema s = getSchema(schema);
        return s.addEventListener(when, listener);
    }
    
    /**
     * Removes an event listener
     * @param schema
     * @param when
     * @param listener 
     */
    public void removEventListener(String schema, EventFireTime when, DataManagerEventListener listener){
        RelationalSchema s = getSchema(schema);
        s.removeEventListener(when, listener);
       
    }


    public void logDebug(Object message){
        log.debug(message);
    }
    
    public void logInfo(Object message){
        log.info(message);
    }
    
    public void logWarn(Object message){
        log.warn(message);
    }
    
    public void logError(Object message){
        log.error(message);
    }
    
    public void logError(Object message, Throwable t){
        log.error(message, t);
    }

}