/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql;


import com.screenscraper.datamanager.sql.util.*;
import com.screenscraper.datamanager.*;
import com.screenscraper.datamanager.skeleton.BasicSchemaAttr;
import com.screenscraper.datamanager.sql.SqlDatabaseSchema.DatabaseSchemaAttr;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A DataWriter for a generic sql database.  MySql is currently the only fully
 * tested database engine but it should support PostgreSql, Oracle, and SqLite
 * @author ryan
 */
public class SqlDataWriter implements DataWriter {

    private Log log = LogFactory.getLog(DataManager.class);
    //For keeping track of inserted datanodes
    private Set<DataNode> inserted;
    private ConcurrentHashMap<String, DataObject> lastAutoIncrementValues;
 //   private Schemas schemas;
    protected final SqlDataManager dm;
    protected boolean autoIncrementSupported = true;
    private String quoteString;

    /**
     * Creates a generic SqlDataWriter that *should* work for most Sql databases
     */
    public SqlDataWriter(SqlDataManager dm, ConcurrentHashMap lastAutoIncrementValues) {
        Connection con=null;
        try{
          con = dm.getConnection();
        }catch(SQLException e){
            throw new IllegalStateException("Database Connection not available",e);
        }finally{
          try{
           if(con!=null)
              con.close();
          }catch(SQLException e){}  
        }
        this.quoteString=dm.getDatabaseSchema().getAttr(DatabaseSchemaAttr.DatabaseIdentifierQuote);
        this.dm=dm;
        this.lastAutoIncrementValues= lastAutoIncrementValues;
        inserted = new HashSet<DataNode>();
    }
    
    /**
     *
     * @param root
     * @return true if the write was successful
     */
    @Override
    public synchronized boolean write(RootNode root)
    {
        root.getChildren();    
        Connection con = null;          
        try{
            con=dm.getConnection();
            for(DataNode child:root.getChildren()){
                try {
                    con.setAutoCommit(false);
                    write(child, con, autoIncrementSupported);
                } catch (UnsupportedEncodingException ex) {
                    log.error(ex.getMessage(),ex);
                }
            }
            writeSecondPass(root,con,autoIncrementSupported); 
        }catch (SQLException e){
            log.error(e.getMessage(),e);
        }
        finally{
            try{
                con.setAutoCommit(true);
                con.close();
            }catch(Exception e){}
        }
        return true;
    }
    
     private void writeSecondPass(RootNode root, Connection con, boolean autoIncrementSupported){
        Set<DataNode> uninserted = new HashSet<DataNode>();
        for(DataNode n: root.getChildren()){
            getUninserted(n,uninserted);
        }
        
        for(DataNode n: uninserted){
            try{
                if(n.isCommitted()){
                    log.info("writing orphaned DataNode: "+n);
                    this.writeSingleDataNode(n, con, autoIncrementSupported);
                } 
                
            }catch(UnsupportedEncodingException e){
                log.warn("Encoding error: " +e.getMessage());
            }
        }
    }
    
    private void getUninserted(DataNode node, Set<DataNode> uninserted){
        if(!inserted.contains(node)){
            uninserted.add(node);
        }
        //recursively write out the children nodes
         for (DataNode child : node.getChildren()) {
             getUninserted(child,uninserted);
         }        
    }
    
    private void writeSingleDataNode(DataNode node, Connection con, boolean autoincrement) throws UnsupportedEncodingException{
             
        
        node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onWrite, new DataManagerEvent(this,node));             
        if(!node.isAborted()){
         try{  
             RelationalSchema s = node.getSchema();
                //Check each of the nodes foreign key constraints against its parents
                for(DataNode parent : node.getParents()){
                    for(SchemaForeignKey fk:  s.getForeignKeys(parent.getSchema().getName())){
                        for(Entry<String,String> entry: fk.getKeyMap().entrySet()){
                            String childColumn = entry.getKey();
                            String parentColumn = entry.getValue();                    
                            if(node.getValue(childColumn)==null && parent.getValue(parentColumn)!=null){
                                 node.addData(childColumn, parent.getValue(parentColumn));
                            }
                        }
                    }           
                }
                
             Map<String,DataObject> autoGen;
             if(dm.keyConstraintsMet(node) && (node.isMergeable() || node.isUpdateable())){
                 autoGen = update(node, con);
             }else{                         
                 autoGen = insertIgnore(node,con,autoIncrementSupported);
             }
             setAutoGeneratedKey(node, autoGen);
             node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.afterWrite,new DataManagerEvent(this,node)); 
             if(node.isAborted()){
                 log.info("rolling back write for DataNode "+node);
                 if(!con.getAutoCommit()){
                    con.rollback();
                 }
             }else{                         
                 try{
                    con.commit();
                 }catch(Exception e){
                     log.warn("sql commit failed, may not be supported by driver");
                 }
             }                    
         }catch(SQLException e){
             String state = "";
             try{
                state = ((e.getSQLState()==null) ? "": e.getSQLState());
             }catch(Exception e2){
                 log.warn("SQLException.getState not supported");
             }
            //this checks if the insert resulted in a duplicate key, if there is an update string set, execute it
            if(state.startsWith("23")){
                log.warn(e.getMessage()+" for DataNode "+node);
                log.warn("setting node: "+node.getSchema().getName()+" as inserted");
                node.setInserted();
                inserted.add(node);
            }else{             
                log.error("SQLException on write for " + node+"\n"+e.getMessage(), e);
            }
            node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onWriteError, new DataManagerEvent(this,node));                  
         } 
       }
    }

    private void write (DataNode node, Connection con, boolean autoincrement) throws UnsupportedEncodingException
    {
        //this is for if a node occurs on more than one path in the tree
        if(inserted.contains(node)){
            return;
        }
        if (setForeignKeys(node) && node.isCommitted())
        {
             node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onWrite, new DataManagerEvent(this,node));             
             if(!node.isAborted()){
                 try{                     
                     Map<String,DataObject> autoGen;
                     if(dm.keyConstraintsMet(node) && (node.isMergeable() || node.isUpdateable())){
                         autoGen = update(node, con);
                     }else{                         
                         autoGen = insertIgnore(node,con,autoIncrementSupported);
                     }
                     setAutoGeneratedKey(node, autoGen);
                     node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.afterWrite,new DataManagerEvent(this,node)); 
                     if(node.isAborted()){
                         log.info("rolling back write for DataNode "+node);
                         con.rollback();
                     }else{
                         try{
                            con.commit();
                         }catch(Exception e){
                            log.warn("sql commit failed, may not be supported by driver");
                         }
                         //recursively write out the children nodes
                         for (DataNode child : node.getChildren()) {
                             write(child, con, autoincrement);
                         }
                     }                    
                 }catch(SQLException e){
                     String state = "";
                     try{
                        state = ((e.getSQLState()==null) ? "": e.getSQLState());
                     }catch(Exception e2){
                         log.warn("SQLException.getState not supported");
                     }
                    //this checks if the insert resulted in a duplicate key, if there is an update string set, execute it
                    if(state.startsWith("23")){
                        log.warn(e.getMessage()+" for DataNode "+node);
                        log.warn("setting node: "+node.getSchema().getName()+" as inserted");
                        node.setInserted();
                        inserted.add(node);
                    }else{             
                        log.error("SQLException on write for " + node+"\n"+e.getMessage(), e);
                    }
                    node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onWriteError, new DataManagerEvent(this,node));                  
                 }         
             }
       }else if(!node.isCommitted()){
           log.warn("Datanode: "+node.getSchema().getName() + " was never committed, skipping");
       }else{
             int i=0;
             StringBuilder msg = new StringBuilder("[");
            for(DataNode parent: node.getParents()){
                if(!parent.getInserted()){
                    if(i==0){
                        msg.append(parent.getSchema().getName());
                    }else{
                        msg.append(",").append(parent.getSchema().getName());
                    }
                    i++;
                }
            }
            msg.append("]");
            log.info("Write postponed for " + node + ", not all parents are inserted: "+msg.toString());
       }
    }
    private boolean setAutoGeneratedKey(DataNode node, Map<String,DataObject> auto) throws UnsupportedEncodingException
    {
        if(node==null)
            throw new IllegalArgumentException("DataNode null");
        Set<String> columns=node.getSchema().getAutoGeneratedColumns();
        if(auto.isEmpty())
            return false;
        /**
         * this is a hack because hsqldb version 1.8 doesn't report which columns are auto increment
         */
        if(columns.isEmpty())
        {
          //  System.out.println("ERROR no auto increment column for " + node.getSchema().getName());
            return false;
        }

        node.addData(auto);
        log.debug("autogen keys saved for "+node.getSchema().getName());
        SchemaKey primary = node.getSchema().getPrimaryKey();
        DataObject pkAutoGen = null;
        for(String column : primary.getColumns()){
            if(auto.containsKey(column)){
                pkAutoGen = auto.get(column);
                break;
            }
        }
        lastAutoIncrementValues.put(node.getSchema().getName(), pkAutoGen);
        return true;
    }

    protected Map<String,DataObject> retrieveLastKey(PreparedStatement ps, DataNode n) throws SQLException
    {
        RelationalSchema s = n.getSchema();

        Map<String,DataObject> ret = new HashMap<String,DataObject>();        
        
        try
        {
            //key was already set at write, get it from DataNode
            SchemaKey pk =  n.getSchema().getPrimaryKey();
            if(dm.constraintMet(n,pk)){
                for(String c: pk.getColumns()){
                    ret.put(c, n.getValue(c));
                }
            }
            else{                
                Set<String> aiColumns = n.getSchema().getAutoGeneratedColumns();
                SchemaKey primary = s.getPrimaryKey();
                List<String> pkColumns = primary.getColumns();
                Iterator<String> itr = pkColumns.iterator();
                while(itr.hasNext()){
                    if(!aiColumns.contains(itr.next())){
                        itr.remove();
                    }
                }
                log.debug("saving auto-generated columns "+aiColumns + " back into DataNode");
                if(!pkColumns.isEmpty()){
                ResultSet rs = ps.getGeneratedKeys();
                    if(rs.next()){
                        for(String name : pkColumns){             
                            DataObject o = s.convertToDataObject(name, rs.getObject(1));
                            ret.put(name, o);                            
                        }                    
                    } 
                }
            }
                    
        }
        catch(SQLException e)
        {
            log.error("Exception in auto generated key retrieval for " + n.getSchema().getName(), e);
        }
        if(!ret.isEmpty()){
            log.info("Retrieved auto-generated keys: "+ ret+ " for table "+n.getSchema().getName());
        }
        return ret;
       
    }


    /*
     * returns a Map with all the data that needs to be merged
     */
    private Map<String,DataObject> mergeData(DataNode n, Map<String, DataObject> prevData)
    {
        Map<String,DataObject> returnMap = new HashMap<String,DataObject>();
        Map<String,DataObject> newData = n.getValues();
        for(String key: newData.keySet())
        {
            if(!prevData.containsKey(key))
                returnMap.put(key, newData.get(key));
        }
        return returnMap;
    }

    /*
     * returns a Map with all the data that needs to be updated
     */
    private Map<String,DataObject> updateData(DataNode n, Map<String, DataObject> prevData)
    {
        Map<String,DataObject> returnMap = new HashMap<String,DataObject>();
        Map<String,DataObject> newData = n.getValues();
        for(String key: newData.keySet())
        {
            DataObject value = prevData.get(key);
            if(value!=null && !value.getObject().equals(newData.get(key).getObject()))
            {
                returnMap.put(key, newData.get(key));
            }
        }
        return returnMap;
    }

    private Map<String,DataObject> getPKData(DataNode n, Map<String, DataObject> prevData){
        List<String> pkColumns = n.getSchema().getPrimaryKey().getColumns();
        Map<String,DataObject> pkVals = new HashMap<String,DataObject>();
        for(String pk: pkColumns){
            pkVals.put(pk, prevData.get(pk));
        }
        return pkVals;
    }

    private Map<String,DataObject> insertIgnore(DataNode node, Connection conn, boolean autoincrement) throws SQLException
    {
        Map<String,DataObject> generatedData= new HashMap<String,DataObject>();
        if(node.getInserted()){
            inserted.add(node);
            return generatedData;
        }
        DmPreparedStatement ps = null;

        dm.logDebug("*** INSERTING DataNode " + node.getSchema().getName());        
        node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onInsert, new DataManagerEvent(this,node));        
        //ps=psbuilder.getPreparedStatement(dm.getConnection(), node, PSBuilder.INSERT, autoIncrementSupported);
        ps=PSBuilder.getInsert(conn, node, quoteString,autoincrement);
        if(!dm.validateData(node)){
            log.warn("Not all column constraints are met for insert of "+node);
            node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onWriteError, new DataManagerEvent(this,node));
            return generatedData;
            
        }else if(!dm.fkConstraintsMet(node)){
            log.warn("Not all foreign key constraints are met for insert of "+node);
            node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onWriteError, new DataManagerEvent(this,node));
            return generatedData;
            
        }else if(ps==null){
            log.error("prepared statement is null: "+node);
            node.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onWriteError, new DataManagerEvent(this,node));
            return generatedData;            
        }
        dm.logDebug("\nEXECUTING INSERT QUERY: ");
        dm.logDebug("\""+ ps.getSql()+ "\"");
        dm.logDebug("VALUES: ");
        for(DataObject o : ps.getData())
        {
        String value = (o==null || o.getObject()==null) ? "" : String.valueOf(o);               
        if(value.length()>50)
            value=value.substring(0, 50) + " (value truncated for log)";
            value = value.replaceAll("\r\n", " ").replaceAll("\n", " ");
            log.debug("\t" + value);
        }          

        ps.executeUpdate();
        generatedData = retrieveLastKey(ps, node);       
        inserted.add(node);
        node.setInserted();
        log.debug("*** END WRITE FOR DataNode " +node.getSchema().getName());
        return generatedData;
    }

    private Map<String,DataObject> update(DataNode n, Connection con) throws UnsupportedEncodingException, SQLException{
        
        if(n.getInserted()){
             inserted.add(n);
            return new HashMap<String,DataObject>();           
        }
        //Select existing data base on keys
        DmPreparedStatement ps = PSBuilder.getSelect(dm.getDatabaseSchema().getAttr(DatabaseSchemaAttr.Vendor),con, n, quoteString);
        log.debug("\nEXECUTING LOOKUP QUERY: ");
        log.debug("\""+ ps.getSql()+ "\"");
        log.debug("VALUES: ");
        for(DataObject o : ps.getData())
        {
            String value = (o==null || o.getObject()==null) ? "" : String.valueOf(o);
            if(value.length()>50)
                value=value.substring(0, 50) + " (value truncated for log)";
            value = value.replaceAll("\r\n", " ").replaceAll("\n", " ");
            log.debug("\t" + value);
        }
        ResultSet rs = ps.executeQuery();

        if(rs.next()){

            n.getSchema().fireEvent(DataManagerEventSource.EventFireTime.onUpdate, new DataManagerEvent(this,n));
            //exisig is the data that is already in the database
            Map<String,DataObject> existing = QueryUtils.saveRowAsMap(rs);
            log.debug("existing data found for table"+n.getSchema().getName()+":\n "+existing);
            Map<String,DataObject> newData = new HashMap<String,DataObject>();
            //add the data that needs to be updated
            if(n.isUpdateable()){
                newData.putAll(this.updateData(n, existing));
            }
            //add the data that needs to be merged
            if(n.isMergeable()){
                newData.putAll(this.mergeData(n, existing));
            }
            if(newData.isEmpty()){
                log.debug("No value updates for "+ n.getSchema().getName());
                newData.putAll(getPKData(n,existing));                
                inserted.add(n);                
                return newData;
            }else{
                log.debug("Updating values "+newData+ " for table "+n.getSchema().getName());                
                newData.putAll(getPKData(n,existing));
                //QueryUtils.updateResultSet(rs, newData);
                //create a new DataNode to hold update data
                DataNode nUpdate = dm.getNewDataNode(n.getSchema().getName());
                nUpdate.addData(newData);
                DmPreparedStatement psUpdate = PSBuilder.getUpdate(con, nUpdate, quoteString);
                log.debug("\nEXECUTING UPDATE QUERY: ");
                log.debug("\""+ psUpdate.getSql()+ "\"");
                log.debug("VALUES: ");
                for(DataObject o : psUpdate.getData())
                {
                    String value = (o==null || o.getObject()==null) ? "" : String.valueOf(o);
                    if(value.length()>50)
                        value=value.substring(0, 50) + " (value truncated for log)";
                    value = value.replaceAll("\r\n", " ").replaceAll("\n", " ");
                    log.debug("\t" + value);
                }
                psUpdate.executeUpdate();
                rs.close();
                ps.close();                
                //add it as being inserted
                inserted.add(n);
                n.setInserted();
                //return the data we looked up
                return newData;
            }
        }else{            
            rs.close();
            ps.close();
            //it wasn't in the database, run regular insert
            return insertIgnore(n, con, autoIncrementSupported);
        }    
    }

    /**
     * sets all the foreign keys from the parent DataNodes
     * @param node
     * @return
     */
    private boolean setForeignKeys(DataNode node) {

        for (DataNode parent : node.getParents()) {
            if (!inserted.contains(parent) && parent.isCommitted()) {
                dm.logDebug(node+ " Parent node " +parent.getSchema().getName()+" not yet inserted");
                return false;
            }
        }
        RelationalSchema s = node.getSchema();
        //Check each of the nodes foreign key constraints against its parents
        for(DataNode parent : node.getParents()){
            for(SchemaForeignKey fk:  s.getForeignKeys(parent.getSchema().getName())){
                for(Entry<String,String> entry: fk.getKeyMap().entrySet()){
                    String childColumn = entry.getKey();
                    String parentColumn = entry.getValue();                    
                    if(node.getValue(childColumn)==null && parent.getValue(parentColumn)!=null)                    {
                         node.addData(childColumn, parent.getValue(parentColumn));
                    }else if(node.getValue(childColumn)==null && 
                            node.getSchema().getAttr(childColumn, BasicSchemaAttr.isNullable).equals(BasicSchemaAttr.Option.no.name()))
                    {
                        log.info("Parent value constraint failed for "+fk+"::"+entry);
                        log.info("Child value is: "+node.getValue(childColumn));
                        log.info("Parent value is: "+parent.getValue(parentColumn));
                        log.info(parent);
                        return false;
                    }
                }
            }           
        }
        return true;
    }

    @Override
    public void startWriting(String startUp) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void finishWriting(String finishUp) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized void close() {
    }    
}