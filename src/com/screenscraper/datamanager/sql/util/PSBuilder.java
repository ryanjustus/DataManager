/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql.util;

import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.SchemaKey;
import com.screenscraper.datamanager.DatabaseSchema;
import com.screenscraper.datamanager.skeleton.*;
import com.screenscraper.datamanager.sql.SqlTableSchema;
import java.io.UnsupportedEncodingException;

import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

 /**
 * @author ryan
 */
public class PSBuilder {

    /**
     * Generate update query based on data in node.
     * Preconditions: n must have primary key data set.
     * @param con
     * @param n
     * @param quote
     * @return
     * @throws SQLException
     */
    public static DmPreparedStatement getUpdate(Connection con, DataNode n, String quote) throws SQLException
    {
        SqlTableSchema s = (SqlTableSchema)n.getSchema();
        ArrayList<DataObject> data = new ArrayList<DataObject>();
        StringBuilder sql = new StringBuilder("UPDATE " +quote+ s.getName() +quote+ " SET ");
        int index=1;
        List<String> pks = n.getSchema().getPrimaryKey().getColumns();
        Set<String> columns = n.getValues().keySet();
        columns.removeAll(pks);
        Iterator<String> itr = columns.iterator();
        while(itr.hasNext())
        {           
            String column = itr.next();
            sql.append(quote).append(column).append(quote).append("=?");
             if(itr.hasNext()){
                 sql.append(", ");
             }
             data.add(n.getValue(column));
        }
        
        sql.append(" WHERE ");
        Iterator<String> pkItr = pks.iterator();
        while(pkItr.hasNext())
        {
            String pk = pkItr.next();
            data.add(n.getValue(pk));
            sql.append(quote).append(pk).append(quote).append("=?");
            if(pkItr.hasNext())
                sql.append(" AND ");
            index++;
        }
        DmPreparedStatement ps = new DmPreparedStatement(con,sql);
        ps.setData(data);
        return ps;
    }

  
    public static DmPreparedStatement getInsert(Connection con, DataNode n, String quote, boolean autoincrement) throws SQLException
    {
        SqlTableSchema s = (SqlTableSchema)n.getSchema();
        ArrayList<DataObject> data = new ArrayList<DataObject>();
        StringBuilder sql = new StringBuilder("INSERT INTO "+quote + s.getName() + quote+" (");
        StringBuilder values = new StringBuilder("");
        int index=1;

        Iterator<Entry<String,DataObject>> valueItr = n.getValues().entrySet().iterator();
        while(valueItr.hasNext())
        {
            Entry<String,DataObject> dt = valueItr.next();
            String column = dt.getKey();
            data.add(dt.getValue());
            sql.append(quote).append(column).append(quote);
            values.append("?");
            if(valueItr.hasNext())
            {
                sql.append(",");
                values.append(",");
            }
            index++;
        }
       
        sql.append(") VALUES(").append(values).append(")");
        //log.info("sql: "+ sql);
        DmPreparedStatement ps;
        if(autoincrement)
        {            
           Set<String> autoColumns = ((SqlTableSchema)n.getSchema()).getAutoGeneratedColumns();
           String[] acArray = new String[autoColumns.size()];
           int i=0;
           for(String c: autoColumns){
               acArray[i]=c;
               i++;
           }
           if(!autoColumns.isEmpty()){
                ps = new DmPreparedStatement(con, sql, acArray);
           }else
                ps = new DmPreparedStatement(con,sql);
        }
        else
            ps = new DmPreparedStatement(con, sql);
        ps.setData(data);
        return ps;
    }

    public static DmPreparedStatement getSelect(String vendor, Connection con, DataNode n, String quote) throws UnsupportedEncodingException, SQLException{
        DatabaseSchema ss = new DatabaseSchema();
        ss.addRelationalSchema(n.getSchema());
        SqlLookup lookup;
        if(vendor.contains("Microsoft")){
            lookup = new MsSqlLookup(ss);
        }else{
            lookup = new SqlLookup(ss);
        }
        Set<String> columns = n.getSchema().getColumns();
        columns.removeAll(n.getSchema().getPrimaryKey().getColumns());

        for(SchemaKey key : n.getSchema().getUniqueIndexes()){
            columns.removeAll(key.getColumns());
        }
      //  lookup.setForUpdate(columns);

        lookup.addSelectColumns(n.getSchema().getName(),n.getSchema().getColumns());
        //add pk constraint
        SchemaKey pk = n.getSchema().getPrimaryKey();
        if(pk==null){
            throw new IllegalStateException("primary key must be defined in database for SqlLookup " + n.getSchema().getName());
        }
        
        Map pkC = getConstraintMap(n,pk);
        lookup.addConstraint(n.getSchema().getName(),pkC);

        //for each unique index add constraint
        for(SchemaKey idx : n.getSchema().getUniqueIndexes()){
            Map<String,DataObject> iC = getConstraintMap(n,idx);
            boolean hasNull = false;
            for(DataObject value : iC.values()){
                if(value==null || value.getObject()==null){
                    hasNull=true;
                    break;
                }
            }
            if(!hasNull){
                lookup.addConstraint(n.getSchema().getName(), iC);
            }
        }
        lookup.setLimit(1);
        return lookup.getPreparedStatement(con);
    }

    private static Map<String,DataObject> getConstraintMap(DataNode n, SchemaKey key){
        Map<String,DataObject> ret = new HashMap<String,DataObject>();
        for(String column : key.getColumns()){
            DataObject val = n.getValue(column);
            //if we don't have a value set and it is not null then don't add it to the lookup
            if((val==null || val.getObject()==null)){
                continue;
            }

            DataObject obj = n.getSchema().convertToDataObject(column, n.getValue(column));
            ret.put(column, obj);
        }
        return ret;
    }

}