/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql.util;

import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataObject.ObjectType;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * Collection of static methods useful when writing/retrieving data from an
 * sql database
 * @author ryan
 */
public class QueryUtils
{
    /* Utility methods */
    public static boolean isApproximatelyNull( String value )
    {
        return ( value == null || value.equals( "null" ) || value.trim().equals( "" ) );
    }

    public static String tidy( Object o )
    {    // Basic tidying for an object (also called from `prepForDB`)
        String query = o.toString();
        query = query.replaceAll( "'", "\\\\'" );
        return query.trim();
    }


    public static String prepForDB( Object value, String prependValue )
    {    // Ensures that null values go into the sql as the string "null", for true database null values
        if( value == null )
            return "null";
        String finalValue = tidy( value );
        if( finalValue.equals( "" ) )
            return "null";

        return new StringBuilder( prependValue ) .append( "'" ).append( finalValue ).append( "'" ).toString();
    }


    public static Map<String,DataObject> saveRowAsMap(ResultSet rs) throws SQLException
    {
        ResultSetMetaData meta= rs.getMetaData();
        Map<String,DataObject> data = new HashMap<String,DataObject>();
        int columnCount= meta.getColumnCount();
        for(int i=1;i<=columnCount;i++)
        {
           int type = meta.getColumnType(i);
           //System.out.println("***** DATABASE TYPE: " + JavaSql.sqlTypeToName(type));
           Object o = null;
           try
           {
            o = rs.getObject(i);
           }
           catch(SQLException e){}
           if(o==null)
               continue;

           String columnName = meta.getColumnName(i);
           ObjectType objectType = JavaSql.sqlToDataObjectTypeConvert(meta.getColumnType(i));
           meta.getColumnType(i);
           DataObject dataObject = new DataObject(o,objectType);
           data.put(columnName,dataObject);
        }
        //log.info(data);
        return data;
    }

    public static boolean isValidSqlName(String name)
    {
        if(name.matches("[\\w]+"))
        {
            return true;
        }
        return false;
    }

   
}
