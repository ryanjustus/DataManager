/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql;

import com.screenscraper.datamanager.*;
import com.screenscraper.datamanager.sql.util.DmPreparedStatement;
import com.screenscraper.datamanager.sql.util.MsSqlLookup;
import com.screenscraper.datamanager.sql.util.QueryUtils;
import com.screenscraper.datamanager.sql.util.SqlLookup;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A DataFilter designed to filter out duplicate records in a database on insertion
 * @author ryan
 */
public class SqlDuplicateFilter{

    String filterTable;
    Map<String,Set<String>>defs;
    DatabaseSchema schemas;
    SqlDataManager dm;
    String filterName;
    private Log log = LogFactory.getLog(SqlDuplicateFilter.class);

    public static SqlDuplicateFilter register(String table, final SqlDataManager dm)
    {
        final SqlDuplicateFilter f = new SqlDuplicateFilter(dm);
        f.setFilterTable(table);
        dm.getSchema(table).addEventListener(
            DataManagerEventSource.EventFireTime.onWrite, new DataManagerEventListener(){
            public void handleEvent(DataManagerEvent event) {
                f.run(event.getDataNode());
            }
        });
        return f;
    }

    private SqlDuplicateFilter(SqlDataManager dm)
    {
        schemas = new DatabaseSchema();
        for(RelationalSchema s: dm.getDatabaseSchema().getRelationalSchemas()){
            schemas.addRelationalSchema(s);
        }
        this.dm=dm;
        filterName = "";
        defs = new HashMap<String,Set<String>>();
    }


    public String getFilterName()
    {
        return filterName;
    }

    private boolean isValidColumn(String table, String column)
    {
        RelationalSchema s = schemas.getRelationalSchema(table);
        if(s==null)
            return false;
        if(!s.getColumns().contains(column))
            return false;
        return true;
    }

    /**
     * Sets the primary table for which we are trying to find matches for the pk
     * For example if we have a table "products" and we want to try to find duplicates
     * for each item listed in it then "products" would be the primary table.  Each
     * filter can only have one primary table.
     * @param table
     */
    private void setFilterTable(String table)
    {
        RelationalSchema s = schemas.getRelationalSchema(table);
        if(s!=null)
            this.filterTable = table;
        else
            throw new IllegalArgumentException("invalid sql table: " + table);
    }

    
    /**
     *
     * @param column must be formatted as "table.column", for example if the table
     * name is "products" and the column is price it would be "products.price"
     */
    public void addConstraint(String column)
    {
        String[] parts = column.split("\\.");
        if(parts.length==2)
        {
            String c = parts[1];
            String t = parts[0];
            addConstraint(t,c);
        }
    }

    /**
     * @param columns each String must be formatted as "table.column", for example
     * if the table name is "products" and the column is price it would be
     * "products.price"
     */
    public void addConstraint(Set<String> columns)
    {
        for(String column: columns)
            addConstraint(column);
    }

    /**
     * create a match definition
     * @param table
     * @param column
     */
    public void addConstraint(String table, String column)
    {
        if(table==null || column == null)
            throw new IllegalArgumentException("table and column cannot be null");
        if(isValidColumn(table,column))
        {
           Set<String> columns = defs.get(table);
           if(columns==null)
           {
               columns = new HashSet<String>();
               defs.put(table, columns);
           }
           columns.add(column);
        }
        else
           throw new IllegalArgumentException("invalid column name: " + column);
    }

    /**
     * apply the filter to the datanodes.  This will query the database and fill
     * in values for the primary keys on matching nodes
     * @param root
     * @return true if the write was successful
     */
    public boolean run(DataNode node) {
        //log.debug("Running filter "+ filterName);
        Connection con = null;
        try {
            con = dm.getConnection();
            if (con == null || con.isClosed()) {
                throw new IllegalStateException("Sql connection invalid");
            }
                  
            //first add the primary Select column
            List<String> pks = node.getSchema().getPrimaryKey().getColumns();
            try
            {
                runFilter(node, pks, con);
            }
            catch(Exception e)
            {
                log.error("SqlFilterError", e);
            }
            
         } catch (SQLException ex) {
            log.error("SqlFilterError", ex);
        }
        finally
        {
            try{con.close();}
            catch(Exception e){}
        }
        return false;
    }

    void runFilter(DataNode node, List<String> pks, Connection con) throws UnsupportedEncodingException
    {

        //log.debug("filtering node " + node.getSchema().getName() + " " + node.getValues());
        //check if pk columns are null
        boolean pkNull = false;

        for(String pk: pks)
        {
            if(node.getValue(pk)==null)
            {
                pkNull = true;
                break;
            }
        }
        if(!pkNull)
        {
            //log.debug("pk already set, exiting filter");
            return;
        }

        SqlLookup lookup;
        if(dm.getDatabaseSchema().getAttr(SqlDatabaseSchema.DatabaseSchemaAttr.Vendor).contains("Microsoft")){
            lookup = new MsSqlLookup(schemas);
        }else{
            lookup = new SqlLookup(schemas);
        }
        
        lookup.setLimit(1);

        lookup.addSelectColumns(node.getSchema().getName(), pks);


        Map primaryLookupData = new HashMap<String,DataObject>();
        Set<String> primaryDataColumns = this.defs.get(node.getSchema().getName());

        for(String column: primaryDataColumns)
        {
            DataObject value = node.getValue(column);
            if(value==null)
            {
                //log.debug("one or more constraint values is null, exiting filter");
                return;
            }
            primaryLookupData.put(column,value);
        }
        lookup.addConstraint(filterTable, primaryLookupData);

        node.getValue(filterTable);

        
        for(String table: defs.keySet())
        {
            if(table.equals(node.getSchema().getName()))
                continue;
            Set<DataNode> children = node.getChildren(table);

            for(DataNode child: children)
            {
                Map secondaryLookupData = new HashMap<String,DataObject>();
                Set<String> secondaryDataColumns= defs.get(table);
                for(String column: secondaryDataColumns)
                {
                    DataObject value = child.getValue(column);
                    if(value==null)
                    {
                        return;
                    }
                    secondaryLookupData.put(column, value);
                }
                lookup.addConstraint(child.getSchema().getName(), secondaryLookupData);
            }
        }
        try
        {
            DmPreparedStatement ps = lookup.getPreparedStatement(con);
            log.debug("EXECUTING DUPLICATEFILTER LOOKUP QUERY\n " + ps.getSql() + "\n" + ps.getData());
            ResultSet rs = ps.executeQuery();
            if(rs.next())
            {
                Map<String,DataObject> pkLookupData = QueryUtils.saveRowAsMap(rs);
                //log.debug("matched pk " + pkLookupData);
                node.addData(pkLookupData);
            }
            else
            {
               //log.debug("no duplicate match found");
            }
            ps.close();
        }
        
        catch(SQLException e)
        {
            log.error("error on lookup", e);
        }
    }
}