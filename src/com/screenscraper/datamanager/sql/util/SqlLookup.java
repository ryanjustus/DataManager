/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql.util;

import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.DatabaseSchema;
import java.sql.*;
import com.screenscraper.datamanager.skeleton.*;
import com.screenscraper.datamanager.util.DataStructureUtils;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.Map.Entry;

/**
 * A class which allows for the programatic construction of an Sql query for
 * looking up data out of a database.
 * @author ryan
 */
public class SqlLookup {


    protected int offset;
    protected int limit;
    protected int orderDirection;
    protected int groupDirection;
    protected int duplicateAction;
    protected boolean built;
    protected ArrayList<String[]> order;
    protected String[] groupBy;


    protected DmPreparedStatement ps;
    protected Map<String,List<DataNode>> constraints;
    protected Map<String,Set<String>> lookups;
    protected DatabaseSchema schemas;
    protected boolean forUpdate;
    protected Set<String> updateColumns;


    public enum Order{
        Ascending,
        Desc,
        None
    };

    public enum Distinct{
        All,
        Distinct,
        DistinctRow
    }
    
    public static final int ASC = 1;
    public static final int DESC = -1;
    public static final int NO_ORDER = 0;
    public static final int ALL = 0;
    public static final int DISTINCT = 1;
    public static final int DISTINCTROW = 2;

    /**
     * Creates a new SqlLookup
     * @param schemas
     */
    public SqlLookup(DatabaseSchema schemas)
    {
        offset=-1;
        limit=-1;
        order = new ArrayList<String[]>();
        groupBy = new String[2];
        orderDirection = SqlLookup.NO_ORDER;
        groupDirection = SqlLookup.NO_ORDER;
        duplicateAction = SqlLookup.ALL;
        ps=null;
        built = false;
        constraints = new HashMap<String,List<DataNode>>();
        lookups = new HashMap<String, Set<String>>();
        this.schemas=schemas;
    }

   /**
    * retrieves the completed PreparedStatement.
    * @param con
    * @return DmPreparedStatement for lookup
    * @throws SQLException
    */
    public DmPreparedStatement getPreparedStatement(Connection con) throws SQLException
    {
        if(!built)
           this.buildQuery(con);
        if(ps==null)
            throw new IllegalStateException("error ps is null");
        return ps;

    }

    /**
     * add columns you wish to select with the statement
     * @param table
     * @param selectColumns
     */
    public void addSelectColumns(String table, Collection<String> selectColumns)
    {
        RelationalSchema schema = schemas.getRelationalSchema(table);
        if(selectColumns==null)
            selectColumns = new HashSet<String>();
        if(built)
            throw new IllegalStateException("PreparedStatement by calling getPreparedStatement already built, no more data can be added");
        for(String column: selectColumns)
        {
            if(!schema.getColumns().contains(column))
            {
                throw new IllegalArgumentException("column " + column + " in selectColumns is invalid");
            }
        }
        Set<String> lookupColumns = lookups.get(table);
        if(lookupColumns==null)
        {
            lookupColumns = new HashSet<String>();
            lookups.put(table, lookupColumns);
        }
        lookupColumns.addAll(selectColumns);
    }

    public void addConstraint(String table, Map<String,? extends Object> whereData) throws UnsupportedEncodingException
    {
        RelationalSchema schema = schemas.getRelationalSchema(table);
        //add select columns
        if(whereData==null || whereData.isEmpty())
            return;
        
        List<DataNode> constraintList = constraints.get(schema.getName());
        if(constraintList == null)
        {
            constraintList = new ArrayList<DataNode>();
            constraints.put(schema.getName(), constraintList);
        }
        DataNode n = new DataNode(schema);
        n.addData(whereData);
        constraintList.add(n);
    }

    /**
     * add constraints (where clause) to the query
     * @param table
     * @param column
     * @param value
     * @throws UnsupportedEncodingException
     */
    public void addConstraint(String table, String column, Object value) throws UnsupportedEncodingException
    {
        Map data = new HashMap<String, Object>();
        data.put(column, value);
        addConstraint(table,data);
    }

    /**
     * add order by to the query
     * @param schema
     * @param column
     */
    public void addOrder(BasicSchema schema, String column)
    {
        if(built)
            throw new IllegalStateException("PreparedStatement already built, no more data can be added");
        String table = schema.getName();
        if(schema!=null && schema.getColumns().contains(column))
        {
            String[] data = {table,column};
            order.add(data);
        }
        else
        {
            throw new IllegalArgumentException("Unknown column "+ column + " for table "+ table );
        }
    }

    /**
     * add group by to the query
     * @param schema
     * @param column
     */
    public void addGroupBy(BasicSchema schema, String column)
    {
        if(built)
            throw new IllegalStateException("PreparedStatement already built, no more data can be added");
        String table = schema.getName();
        if(schema!=null && schema.getColumns().contains(column))
        {
            groupBy[0]=table;
            groupBy[1]=column;
        }
        else
        {
            throw new IllegalArgumentException("Unknown column "+ column + " for table "+ table );
        }
    }

    /**
     * set the group direction(ascending or descending)
     * @param order
     */
    public void setGroupDirection(int order)
    {
        if(built)
            throw new IllegalStateException("PreparedStatement already built, no more data can be added");
        this.groupDirection=order;
    }

    /**
     * set what to do on a duplicate
     * @param duplicateAction
     */
    public void setDuplicateAction(int duplicateAction)
    {
        if(duplicateAction==SqlLookup.DISTINCT || duplicateAction==SqlLookup.DISTINCTROW)
            this.duplicateAction=duplicateAction;
        else
            this.duplicateAction=SqlLookup.ALL;

    }

    /**
     * set the order by direction (ascending or descending)
     * @param order
     */
    public void setOrderDirection(int order)
    {
        if(built)
            throw new IllegalStateException("PreparedStatement already built, no more data can be added");
        this.orderDirection=order;
    }

    /**
     * set a limit for the number of rows returned
     * @param limit
     */
    public void setLimit(int limit)
    {
        if(built)
            throw new IllegalStateException("PreparedStatement already built, no more data can be added");
        if(limit>0)
            this.limit=limit;
        else
            throw new IllegalArgumentException("limit cannot be negative of zero");
    }

    /**
     * set the offset of the query
     * @param offset
     */
    public void setOffset(int offset)
    {
        if(built)
            throw new IllegalStateException("PreparedStatement already built, no more data can be added");
        if(offset>=0)
            this.limit=offset;
        else
            throw new IllegalArgumentException("offset cannot be negative");
    }

    /**
     * build the query
     * @param con
     * @throws SQLException
     */
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
        StringBuilder query = new StringBuilder("SELECT " + duplicateActionString + columnString + " FROM " + tableString);
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
        if(limit>=1)
          query.append(" LIMIT ").append(limit);
        if(offset>=0)
            query.append(" OFFSET ").append(offset);
        if(forUpdate)
            query.append(" FOR UPDATE");
        String queryString = query.toString();
        DmPreparedStatement ps = new DmPreparedStatement(con, queryString);
        ps.setData(whereData);
        this.ps=ps;
    }

    public void setForUpdate(Set<String> columns){
        this.forUpdate=true;
        this.updateColumns = columns;
    }

    protected WhereContainer buildWhere()
    {
        //get all the non-join where data
        ArrayList<String> subWheres = new ArrayList<String>(this.constraints.size());
        ArrayList<DataObject> whereData = new ArrayList<DataObject>();

        Set<String> whereTables = new HashSet<String>();
        for(Entry<String,List<DataNode>> e : constraints.entrySet())
        {
            String table = e.getKey();
            int lookupsSize = e.getValue().size();
            StringBuilder lookupPart = new StringBuilder("");
            if(lookupsSize>1)
                lookupPart.append("(");
            Iterator<DataNode> dataNodeItr = e.getValue().iterator();
            while(dataNodeItr.hasNext())
            {

               DataNode n = dataNodeItr.next();
               int columnIndex = 0;
               int columnMax = n.getValues().size();

               Iterator<Entry<String,DataObject>> datumItr = n.getValues().entrySet().iterator();
               int numData = n.getValues().keySet().size();
               if(numData>1)
                   lookupPart.append("(");
               while(datumItr.hasNext())
               {
                   Entry<String,DataObject> datum = datumItr.next();
                   columnIndex++;
                   DataObject o = datum.getValue();
                   String column = datum.getKey();
                   if(o==null || o.getObject()==null){
                      lookupPart.append(table).append(".").append(column).append(" IS NULL"); 
                   }else{
                       lookupPart.append(table).append(".").append(column).append("=?");   
                       whereData.add(o);
                   }                   
                   whereTables.add(table);
                   if(datumItr.hasNext())
                       lookupPart.append(" AND ");                   
               }
               if(numData>1){
                   lookupPart.append(")");
                   
               }
               if(dataNodeItr.hasNext() && numData>0)
               {
                   lookupPart.append(" OR ");
               }
            }
            if(lookupsSize>1)
                lookupPart.append(")");
            subWheres.add(lookupPart.toString());
        }

        //join all the subWhere parts
        String whereString = DataStructureUtils.join(" AND ",subWheres);
        return new WhereContainer(whereString, whereData,whereTables);
    }

    class WhereContainer
    {
        public String whereString;
        public ArrayList<DataObject> whereData;
        public Set<String> whereTables;
        public WhereContainer(String whereString, ArrayList<DataObject> whereData, Set<String> whereTables)
        {
            this.whereString=whereString;
            this.whereData=whereData;
            this.whereTables=whereTables;
        }
    }
}