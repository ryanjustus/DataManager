/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.screenscraper.datamanager.sql.util;

import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.DatabaseSchema;
import com.screenscraper.datamanager.util.DataStructureUtils;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author ryan
 */
public class MsSqlLookup extends SqlLookup{
    public MsSqlLookup(DatabaseSchema schemas){
        super(schemas);
    }
    

    
       /**
     * build the query
     * @param con
     * @throws SQLException
     */
    @Override
    protected void buildQuery(Connection con) throws SQLException
    {
        if(built)
            throw new IllegalStateException("PreparedStatement already built, cannot build again");
        built=true;

        ArrayList<String[]> selectColumns = new ArrayList<String[]>();
        Set<String> tables = new HashSet<String>();
        Set<String> manyToManyJoinTables = new HashSet<String>();
   

        //build the select columns part of the query
        for(Entry<String,Set<String>> e: this.lookups.entrySet())
        {
            String table = e.getKey();
            Set<String> columns = e.getValue();
            for(String column: columns)
            {
                String[] tc = {table,column};
                selectColumns.add(tc);
            }
            if(!columns.isEmpty())
            {
                tables.add(table);
            }
        }

        //Where portion of the query
        //get the "where" string
        WhereContainer whereContainer = buildWhere();
        String whereString = whereContainer.whereString;
        ArrayList<DataObject> whereData = whereContainer.whereData;
        tables.addAll(whereContainer.whereTables);

        //Determine all the table joins
        // Array Representation: {table,column,fkTable,fkColumn}
        ArrayList<String[]> joinArray = new ArrayList<String[]>();
        
        ArrayList<String[]> possibleManyToMany = new ArrayList<String[]>();
        
        for(String table: tables)
        {
            RelationalSchema s = schemas.getRelationalSchema(table);
            

            Set<SchemaForeignKey> fks = s.getForeignKeys();
            for(SchemaForeignKey key: fks)
            {
                Map<String,String> columns = key.getKeyMap();
                String fkTable = key.getParentSchemaName();
  
                //if the foreign key table is in our list for the select
                if(tables.contains(fkTable))
                {
                    for(Entry<String,String> entry : columns.entrySet()){
                        String[] currKey = {table,entry.getKey(),fkTable,entry.getValue()};
                        joinArray.add(currKey);
                        possibleManyToMany.add(currKey);
                    }
                }
                else
                {
                    for(String[] prevKey : possibleManyToMany)
                    {
                        String possibleFkTable = prevKey[2];
                        String possibleFkColumn = prevKey[3];
                       
                        //if this is the case we have a many to many table, so we add both joins
                        if(fkTable.equals(possibleFkTable))
                        {
                            for(String fkColumn : columns.values()){
                                if(!fkColumn.equals(possibleFkColumn)){
                                    manyToManyJoinTables.add(fkTable);
                                    joinArray.add(prevKey);
                                    joinArray.add(prevKey);
                                }
                            }
                        }                   
                    }
                    
                }
            }
        }
        tables.addAll(manyToManyJoinTables);        
       
        
        //all the from tables
        String tableString = DataStructureUtils.join(",", tables);
        
        //all the select columns
        StringBuilder columns = new StringBuilder("");
        Iterator<String[]> selectItr = selectColumns.iterator();
        while(selectItr.hasNext())
        {
            String[] column = selectItr.next();
            columns.append(column[0]).append(".").append(column[1]);
            if(selectItr.hasNext())
                columns.append(",");
        }
        columns.append("");
        String columnString=columns.toString();

        //assemble the join wheres
        StringBuilder joins = new StringBuilder("");
        Iterator<String[]> joinItr = joinArray.iterator();
        while(joinItr.hasNext())
        {
            String[] key = joinItr.next();
            joins.append(key[0]).append(".").append(key[1]).append("=").append(key[2]).append(".").append(key[3]);
            if(joinItr.hasNext())
                joins.append(" AND ");
        }
        String joinString = joins.toString();

        //assemble the group by
        StringBuilder groupByString = new StringBuilder("");
        if(groupBy[0]!=null && groupBy[1]!=null)
        {
            for(String[] selectColumn: selectColumns)
            {
                if(selectColumn[0].equals(groupBy[0]) && selectColumn[1].equals(groupBy[1]))
                {
                    groupByString.append(" GROUP BY ").append(groupBy[0]).append(".").append(groupBy[1]);
                    if(groupDirection>1)
                        groupByString.append(" ASC");
                    else if(groupDirection<1)
                        groupByString.append(" DESC");
                }
            }
        }

        //assemble the order by
        StringBuilder orderString = new StringBuilder("");
        for(String[] orderData : order)
        {
            String table = orderData[0];
            String column= orderData[1];
            if(tables.contains(table))
            {
                orderString.append(table).append(".").append(column).append(",");
            }
        }
        if(orderDirection>0)
                orderString.append(" ASC");
            else if(orderDirection<0)
                orderString.append(" DESC");

        //set the duplicate action (ALL, DISTINCT, DISTINCTROW)
        String duplicateActionString;
        if(this.duplicateAction==SqlLookup.DISTINCT)
            duplicateActionString="DISTINCT ";
        else if(this.duplicateAction==SqlLookup.DISTINCTROW)
            duplicateActionString="DISTINCTROW ";
        else
            duplicateActionString="";        

        //assemble the query
        StringBuilder query = new StringBuilder("SELECT " + duplicateActionString);
         if(limit>=1)
          query.append(" TOP ").append(limit);
        query.append(" ").append(columnString).append(" FROM ").append(tableString);
        if(joinString.length()>0)
            query.append(" WHERE ").append(joinString);
        if(whereString.length()>0)
            if(joinString.length()==0)
                query.append(" WHERE ").append(whereString);
            else
                query.append(" AND ").append(whereString);
        if(groupByString.length()>0)
            query.append(" GROUP BY ").append(groupByString);
        if(orderString.length()>0)
            query.append(" ORDER BY ").append(orderString);
        if(offset>=0)
            query.append(" OFFSET ").append(offset);
        if(forUpdate)
            query.append(" FOR UPDATE");
        String queryString = query.toString();
        DmPreparedStatement ps = new DmPreparedStatement(con, queryString);
        ps.setData(whereData);
        this.ps=ps;
    }    
}