/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql.util;

import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataObject.ObjectType;


/**
 * Contains static methods for mapping between java.sql.Types and Java objects
 * @author ryan
 */
public class JavaSql {

  /**
   * Maps java.sql.Types to a String representation of their name
   * @param sqlType
   * @return String representation of sqlType
   */
  public static String sqlTypeToName(int sqlType)
    {
        switch (sqlType)
        {
            case java.sql.Types.ARRAY:  return "ARRAY";
            case java.sql.Types.BIGINT:  return "BIGINT";
            case java.sql.Types.BINARY:  return "BINARY";
            case java.sql.Types.BIT:  return "BIT";
            case java.sql.Types.BLOB:  return "BLOB";
            case java.sql.Types.BOOLEAN:  return "BOOLEAN";
            case java.sql.Types.CHAR:  return "CHAR";
            case java.sql.Types.CLOB:  return "CLOB";
            case java.sql.Types.DATALINK:  return "DATALINK";
            case java.sql.Types.DATE:  return "DATE";
            case java.sql.Types.DECIMAL:  return "DECIMAL";
            case java.sql.Types.DISTINCT:  return "DISTINCT";
            case java.sql.Types.DOUBLE:  return "DOUBLE";
            case java.sql.Types.FLOAT:  return "FLOAT";
            case java.sql.Types.INTEGER:  return "INTEGER";
            case java.sql.Types.JAVA_OBJECT:  return "JAVA_OBJECT";
            case java.sql.Types.LONGVARBINARY: return "LONGVARBINARY";
            case java.sql.Types.LONGNVARCHAR: return "LONGVARCHAR";
            case java.sql.Types.LONGVARCHAR:  return "LONGNVARCHAR";
            case java.sql.Types.NCHAR:  return "NCHAR";
            case java.sql.Types.NCLOB:  return "NCLOB";
            case java.sql.Types.NULL:  return "NULL";
            case java.sql.Types.NUMERIC:  return "NUMERIC";
            case java.sql.Types.NVARCHAR:  return "NVARCHAR";
            case java.sql.Types.OTHER:  return "OTHER";
            case java.sql.Types.REAL:  return "REAL";
            case java.sql.Types.REF:  return "REF";
            case java.sql.Types.ROWID:  return "ROWID";
            case java.sql.Types.SMALLINT:  return "SMALLINT";
            case java.sql.Types.SQLXML:  return "SQLXML";
            case java.sql.Types.STRUCT:  return "STRUCT";
            case java.sql.Types.TIME:  return "TIME";
            case java.sql.Types.TIMESTAMP:  return "TIMESTAMP";
            case java.sql.Types.TINYINT:  return "TINYINT";
            case java.sql.Types.VARBINARY:  return "VARBINARY";
            case java.sql.Types.VARCHAR:  return "VARCHAR";

            default: throw new IllegalArgumentException("unknown java.sql.Types type "+ sqlType);
        }
     }

    /**
     * returns a Mapping of the java.sql.Types to a corresponding java class
     * @param jdbcType
     * @return java class corresponding to sqlType
     */
    public static Class sqlTypeToClass(int jdbcType)
    {
            ObjectType t = sqlToDataObjectTypeConvert(jdbcType);
            return t.getClass();
    }

     public static ObjectType sqlToDataObjectTypeConvert(int jdbcType)
    {
        switch (jdbcType)
        {
            case java.sql.Types.CHAR: return DataObject.ObjectType.String;
            case java.sql.Types.VARCHAR: return DataObject.ObjectType.String;
            case java.sql.Types.NVARCHAR: return DataObject.ObjectType.String;
            case java.sql.Types.LONGNVARCHAR: return DataObject.ObjectType.String;
            case java.sql.Types.NUMERIC: return DataObject.ObjectType.BigDecimal;
            case java.sql.Types.DECIMAL: return DataObject.ObjectType.BigDecimal;
            case java.sql.Types.BIT: return DataObject.ObjectType.Boolean;
            case java.sql.Types.TINYINT: return DataObject.ObjectType.Integer;
            case java.sql.Types.SMALLINT: return DataObject.ObjectType.Integer;
            case java.sql.Types.BIGINT: return DataObject.ObjectType.Long;
            case java.sql.Types.REAL: return DataObject.ObjectType.Double;
            case java.sql.Types.FLOAT: return DataObject.ObjectType.Double;
            case java.sql.Types.DOUBLE: return DataObject.ObjectType.Double;
            case java.sql.Types.BINARY: return DataObject.ObjectType.ByteArray;
            case java.sql.Types.VARBINARY: return DataObject.ObjectType.ByteArray;
            case java.sql.Types.LONGVARBINARY: return DataObject.ObjectType.ByteArray;
            case java.sql.Types.DATE: return DataObject.ObjectType.Date;
            case java.sql.Types.TIME: return DataObject.ObjectType.Time;
            case java.sql.Types.TIMESTAMP: return DataObject.ObjectType.Timestamp;
            case java.sql.Types.ARRAY: return DataObject.ObjectType.Object;
            case java.sql.Types.BLOB: return DataObject.ObjectType.ByteArray;
            case java.sql.Types.CLOB: return DataObject.ObjectType.Object;
            case java.sql.Types.INTEGER: return DataObject.ObjectType.Integer;
            case java.sql.Types.JAVA_OBJECT: return DataObject.ObjectType.Object;
            case java.sql.Types.LONGVARCHAR: return DataObject.ObjectType.String;
            case java.sql.Types.OTHER: return DataObject.ObjectType.Object;
            default: throw new IllegalArgumentException("unsupported java.sql.Type "+ jdbcType);
        }
    }
}
