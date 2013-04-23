/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql.util;

import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataObject.ObjectType;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A wrapper for java.sql.PreparedStatement that adds methods for dealing Directly
 * interacting with the DataObject class, as well of methods to retrieve values
 * that were used in the preparation of the PreparedStatement
 * @author ryan
 */
public final class DmPreparedStatement implements java.sql.PreparedStatement{

    private PreparedStatement ps;
    private String sql;
    private TreeMap<Integer,DataObject> data;
    private long executionTime;


    /**
     * Creates a DmPreparedStatement designed for executeQuery (Lookup as opposed to update)
     * java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE and java.sql.ResultSet.CONCUR_UPDATABLE are set
     * PreparedStatement parameters: 
     * @param con
     * @param sql
     * @throws SQLException
     */
    public DmPreparedStatement(Connection con, CharSequence sql) throws SQLException
    {
        PreparedStatement psL = con.prepareStatement(sql.toString(),java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_UPDATABLE);
        //PreparedStatement psL = con.prepareStatement(sql.toString(),java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_UPDATABLE);
        init(psL,sql);
    }

    /**
     * Creates a DmPreparedStatement from the given sql statement with optional retrieval of auto generated keys
     * @param con
     * @param sql
     * @param autoGeneratedKeys
     * @throws SQLException
     */
    public DmPreparedStatement(Connection con, CharSequence sql, int autoGeneratedKeys) throws SQLException
    {
        PreparedStatement ps = con.prepareStatement(sql.toString(), autoGeneratedKeys);
        init(ps, sql);
    }

    public DmPreparedStatement(Connection con, CharSequence sql, String[] pk) throws SQLException {
        PreparedStatement ps = con.prepareStatement(sql.toString(),pk);
        init(ps, sql);
    }

    void init(PreparedStatement ps, CharSequence sql)
    {
        this.executionTime=-1;
        this.ps=ps;
        this.sql=sql.toString();
        data = new TreeMap<Integer,DataObject>();
    }

    /**
     * returns the Sql String that was used to create the PreparedStatement
     * @return String representation of Sql set in the PreparedStatement
     */
    public String getSql()
    {
        return sql;
    }

    public long getRunTime()
    {
        return executionTime;
    }

    /**
     * returns all the set data as a List of DataObjects
     * @return DataObjects that are set in the PreparedStatement
     */
    public List<DataObject> getData()
    {
        ArrayList<DataObject> dataArray = new ArrayList(data.size());
        for(Entry<Integer,DataObject> e: data.entrySet())
        {
            dataArray.add(e.getValue());
        }
        return dataArray;
    }

    /**
     * returns the data set in te ith position as a DataObject
     * @param i
     * @return ith DataObject set in the PreparedStatement
     */
    public DataObject getData(int i)
    {
        return data.get(i);
    }

    /**
     * Set the designated parameter of the given DataNode value.  The driver converts this to its
     * corresponding SQL type when it sends it to the database.
     * @param i
     * @param data
     * @throws SQLException
     */
    public void setData(int i, DataObject data) throws SQLException
    {
        if(data==null || data.getObject()==null) {
            ps.setNull(i, java.sql.Types.NULL);
        }else {
            Object value = data.getObject();
            ObjectType type = data.getType();
	        switch(type){
		        case Object:  ps.setObject(i, value);
			        break;
		        case String: ps.setString(i, (String)value);
			        break;
		        case ByteArray:  ps.setBytes(i, (byte[])value);
			        break;
		        case Byte: ps.setByte(i, (Byte)value);
			        break;
		        case Short: ps.setShort(i, (Short)value);
			        break;
		        case Integer: ps.setInt(i, (Integer)value);
			        break;
		        case Float: ps.setFloat(i,(Float)value);
			        break;
		        case Double: ps.setDouble(i, (Double)value);
			        break;
		        case Long: ps.setLong(i, (Long)value);
			        break;
		        case Boolean: ps.setBoolean(i, (Boolean)value);
			        break;
		        case Timestamp: ps.setTimestamp(i, (java.sql.Timestamp)value);
			        break;
		        case Time:  ps.setTime(i, (java.sql.Time)value);
	                break;
		        case Date: ps.setDate(i, (java.sql.Date)value);
			        break;
                default:
                    ps.setObject(i, value);
            }
        }
       this.data.put(i, data);
    }

    /**
     * Sets the PreparedStatement parameter with data, the List must be in the
     * same order as its corresponding parameters in the PreparedStatement
     * @param data
     * @throws SQLException
     */
    public void setData(List<DataObject> data) throws SQLException
    {
        int i=1;
        for(DataObject value: data)
        {
            setData(i, value);
            i++;
        }
    }

    /**
     * executes the query normally except instead of returning a ResultSet it returns 
     * a List of Map<String columnName, DataObject values>
     * @return List represents rows and Map represents columns of table
     * @throws SQLException
     */
    public List<Map<String,DataObject>> executeQueryGetMapList() throws SQLException
    {
        ArrayList<Map<String,DataObject>> returnVal = new ArrayList<Map<String,DataObject>>();
        ResultSet rs = ps.executeQuery();
        while(rs.next())
        {
            returnVal.add(QueryUtils.saveRowAsMap(rs));
        }
        rs.close();
        return returnVal;
    }

    public void close() throws SQLException
    {
        ps.close();
    }

    public ResultSet executeQuery() throws SQLException {
        long startTime = System.currentTimeMillis();
        ResultSet rs =  ps.executeQuery();
        long endTime = System.currentTimeMillis();
        this.executionTime = endTime-startTime;
        return rs;

    }

    public int executeUpdate() throws SQLException {
        long startTime = System.currentTimeMillis();
        int status =  ps.executeUpdate();
        long endTime = System.currentTimeMillis();
        this.executionTime = endTime-startTime;
        return status;
    }

    public void setNull(int i, int i1) throws SQLException {
       ps.setNull(i, i1);
    }

    public void setBoolean(int i, boolean bln) throws SQLException {
        DataObject o = new DataObject(bln, ObjectType.Boolean);
        data.put(i, o);
        ps.setBoolean(i, bln);
    }

    public void setByte(int i, byte b) throws SQLException {
        DataObject o = new DataObject(b, ObjectType.Byte);
        data.put(i, o);
        ps.setByte(i, b);
    }

    public void setShort(int i, short s) throws SQLException {
        DataObject o = new DataObject(s, ObjectType.Short);
        data.put(i, o);
        ps.setShort(i, s);
    }

    public void setInt(int i, int i1) throws SQLException {
        DataObject o = new DataObject(i1, ObjectType.Integer);
        data.put(i, o);
        ps.setInt(i, i1);
    }

    public void setLong(int i, long l) throws SQLException {
        DataObject o = new DataObject(l, ObjectType.Long);
        data.put(i, o);
        ps.setLong(i, l);
    }

    public void setFloat(int i, float f) throws SQLException {
        DataObject o = new DataObject(f, ObjectType.Float);
        data.put(i, o);
        ps.setFloat(i, f);
    }

    public void setDouble(int i, double d) throws SQLException {
        DataObject o = new DataObject(d, ObjectType.Double);
        data.put(i, o);
        ps.setDouble(i, d);
    }

    public void setBigDecimal(int i, BigDecimal bd) throws SQLException {
        DataObject o = new DataObject(bd, ObjectType.BigDecimal);
        data.put(i, o);
        ps.setBigDecimal(i, bd);
    }

    public void setString(int i, String string) throws SQLException {
        DataObject o = new DataObject(string, ObjectType.String);
        data.put(i, o);
        ps.setString(i, string);
    }

    public void setBytes(int i, byte[] bytes) throws SQLException {
        DataObject o = new DataObject(bytes, ObjectType.ByteArray);
        data.put(i, o);
        ps.setBytes(i, bytes);
    }

    public void setDate(int i, java.sql.Date date) throws SQLException {
        DataObject o = new DataObject(date, ObjectType.Date);
        data.put(i, o);
        ps.setDate(i,date);
    }

    public void setTime(int i, Time time) throws SQLException {
        DataObject o = new DataObject(time, ObjectType.Time);
        data.put(i, o);
        ps.setTime(i,time);
    }

    public void setTimestamp(int i, Timestamp tmstmp) throws SQLException {
        DataObject o = new DataObject(tmstmp, ObjectType.Timestamp);
        data.put(i, o);
        ps.setTimestamp(i,tmstmp);
    }

    public void setAsciiStream(int i, InputStream in, int i1) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setAsciiStream(i, in, i1);
    }

    public void setUnicodeStream(int i, InputStream in, int i1) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setUnicodeStream(i, in, i1);
    }

    public void setBinaryStream(int i, InputStream in, int i1) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setBinaryStream(i, in);
    }

    public void clearParameters() throws SQLException {
        data.clear();
        ps.clearParameters();
    }

    public void setObject(int i, Object o, int i1) throws SQLException {
        DataObject obj = new DataObject(o, ObjectType.Object);
        data.put(i, obj);
        ps.setObject(i, o, i1);
    }

    public void setObject(int i, Object o) throws SQLException {
        DataObject obj = new DataObject(o, ObjectType.Object);
        data.put(i, obj);
        ps.setObject(i, o);
    }

    public boolean execute() throws SQLException {
        long startTime = System.currentTimeMillis();
        boolean returnVal =  ps.execute();
        long endTime = System.currentTimeMillis();
        this.executionTime = endTime-startTime;
        return returnVal;
    }

    public void addBatch() throws SQLException {
        ps.addBatch();
    }

    public void setCharacterStream(int i, Reader reader, int i1) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setCharacterStream(i, reader, i1);
    }

    public void setRef(int i, Ref ref) throws SQLException {
        DataObject o = new DataObject(ref, ObjectType.Object);
        data.put(i, o);
        ps.setRef(i,ref);
    }

    public void setBlob(int i, Blob blob) throws SQLException {
        DataObject o = new DataObject(blob, ObjectType.Object);
        data.put(i, o);
        ps.setBlob(i, blob);
    }

    public void setClob(int i, Clob clob) throws SQLException {
        DataObject o = new DataObject(clob, ObjectType.Object);
        data.put(i, o);
        ps.setClob(i,clob);
    }

    public void setArray(int i, Array array) throws SQLException {
        DataObject o = new DataObject(array, ObjectType.Object);
        data.put(i, o);
        ps.setArray(i, array);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return ps.getMetaData();
    }

    public void setDate(int i, java.sql.Date date, Calendar clndr) throws SQLException {
        DataObject o = new DataObject(date, ObjectType.Date);
        data.put(i, o);
        ps.setDate(i, date,clndr);
    }

    public void setTime(int i, Time time, Calendar clndr) throws SQLException {
        DataObject o = new DataObject(time, ObjectType.Time);
        data.put(i, o);
        ps.setTime(i, time, clndr);
    }

    public void setTimestamp(int i, Timestamp tmstmp, Calendar clndr) throws SQLException {
        DataObject o = new DataObject(tmstmp, ObjectType.Timestamp);
        data.put(i, o);
        ps.setTimestamp(i, tmstmp, clndr);
    }

    public void setNull(int i, int i1, String string) throws SQLException {
        ps.setNull(i, i1,string);
    }

    public void setURL(int i, URL url) throws SQLException {
        DataObject o = new DataObject(url, ObjectType.Object);
        data.put(i, o);
        ps.setURL(i,url);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps.getParameterMetaData();
    }

    public void setRowId(int i, RowId rowid) throws SQLException {
        ps.setRowId(i,rowid);
    }

    public void setNString(int i, String string) throws SQLException {
        DataObject o = new DataObject(string, ObjectType.String);
        data.put(i, o);
        ps.setNString(i,string);
    }

    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setNCharacterStream(i,reader,l);
    }

    public void setNClob(int i, NClob nclob) throws SQLException {
        DataObject o = new DataObject(nclob, ObjectType.Object);
        data.put(i, o);
        ps.setNClob(i,nclob);
    }

    public void setClob(int i, Reader reader, long l) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setClob(i, reader,l);
    }

    public void setBlob(int i, InputStream in, long l) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setBlob(i, in,l);
    }

    public void setNClob(int i, Reader reader, long l) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setNClob(i, reader,l);
    }

    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        DataObject o = new DataObject(sqlxml, ObjectType.Object);
        data.put(i, o);
        ps.setSQLXML(i, sqlxml);
    }

    public void setObject(int i, Object o, int i1, int i2) throws SQLException {
        DataObject obj = new DataObject(o, ObjectType.Object);
        data.put(i, obj);
        ps.setObject(i, o, i1, i2);
    }

    public void setAsciiStream(int i, InputStream in, long l) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setAsciiStream(i, in,l);
    }

    public void setBinaryStream(int i, InputStream in, long l) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setBinaryStream(i, in,l);
    }

    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setCharacterStream(i, reader,l);
    }

    public void setAsciiStream(int i, InputStream in) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setAsciiStream(i, in);
    }

    public void setBinaryStream(int i, InputStream in) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setBinaryStream(i, in);
    }

    public void setCharacterStream(int i, Reader reader) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setCharacterStream(i, reader);
    }

    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setNCharacterStream(i, reader);
    }

    public void setClob(int i, Reader reader) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setClob(i, reader);
    }

    public void setBlob(int i, InputStream in) throws SQLException {
        DataObject o = new DataObject(in, ObjectType.InputStream);
        data.put(i, o);
        ps.setBlob(i, in);
    }

    public void setNClob(int i, Reader reader) throws SQLException {
        DataObject o = new DataObject(reader, ObjectType.Object);
        data.put(i, o);
        ps.setNClob(i, reader);
    }

    public ResultSet executeQuery(String string) throws SQLException {

        long startTime = System.currentTimeMillis();
        ResultSet rs =  ps.executeQuery(string);
        long endTime = System.currentTimeMillis();
        this.executionTime = endTime-startTime;
        return rs;
    }

    public int executeUpdate(String string) throws SQLException {
        long startTime = System.currentTimeMillis();
        int rs =  ps.executeUpdate(string);
        long endTime = System.currentTimeMillis();
        this.executionTime = endTime-startTime;
        return rs;
    }

    public int getMaxFieldSize() throws SQLException {
        return ps.getMaxFieldSize();
    }

    public void setMaxFieldSize(int i) throws SQLException {
        ps.setMaxFieldSize(i);
    }

    public int getMaxRows() throws SQLException {
        return ps.getMaxRows();
    }

    public void setMaxRows(int i) throws SQLException {
        ps.setMaxRows(i);
    }

    public void setEscapeProcessing(boolean bln) throws SQLException {
        ps.setEscapeProcessing(bln);
    }

    public int getQueryTimeout() throws SQLException {
        return ps.getQueryTimeout();
    }

    public void setQueryTimeout(int i) throws SQLException {
        ps.setQueryTimeout(i);
    }

    public void cancel() throws SQLException {
        ps.cancel();
    }

    public SQLWarning getWarnings() throws SQLException {
        return ps.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        ps.clearWarnings();
    }

    public void setCursorName(String string) throws SQLException {
        ps.setCursorName(string);
    }

    public boolean execute(String string) throws SQLException {
        long startTime = System.currentTimeMillis();
        boolean rs =  ps.execute(string);
        long endTime = System.currentTimeMillis();
        this.executionTime = endTime-startTime;
        return rs;
    }

    public ResultSet getResultSet() throws SQLException {
        return ps.getResultSet();
    }

    public int getUpdateCount() throws SQLException {
        return ps.getUpdateCount();
    }

    public boolean getMoreResults() throws SQLException {
        return ps.getMoreResults();
    }

    public void setFetchDirection(int i) throws SQLException {
        ps.setFetchDirection(i);
    }

    public int getFetchDirection() throws SQLException {
        return ps.getFetchDirection();
    }

    public void setFetchSize(int i) throws SQLException {
        ps.setFetchSize(i);
    }

    public int getFetchSize() throws SQLException {
        return ps.getFetchSize();
    }

    public int getResultSetConcurrency() throws SQLException {
        return ps.getResultSetConcurrency();
    }

    public int getResultSetType() throws SQLException {
        return ps.getResultSetType();
    }

    public void addBatch(String string) throws SQLException {
        ps.addBatch();
    }

    public void clearBatch() throws SQLException {
        ps.clearBatch();
    }

    public int[] executeBatch() throws SQLException {
        return ps.executeBatch();
    }

    public Connection getConnection() throws SQLException {
        return ps.getConnection();
    }

    public boolean getMoreResults(int i) throws SQLException {
        return ps.getMoreResults(i);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return ps.getGeneratedKeys();
    }

    public int executeUpdate(String string, int i) throws SQLException {
        return ps.executeUpdate(string, i);
    }

    public int executeUpdate(String string, int[] ints) throws SQLException {
        return ps.executeUpdate(string, ints);
    }

    public int executeUpdate(String string, String[] strings) throws SQLException {
        return ps.executeUpdate(string,strings);
    }

    public boolean execute(String string, int i) throws SQLException {
        return ps.execute(string,i);
    }

    public boolean execute(String string, int[] ints) throws SQLException {
        return ps.execute(string, ints);
    }

    public boolean execute(String string, String[] strings) throws SQLException {
        return ps.execute();
    }

    public int getResultSetHoldability() throws SQLException {
        return ps.getResultSetHoldability();
    }

    public boolean isClosed() throws SQLException {
        return ps.isClosed();
    }

    public void setPoolable(boolean bln) throws SQLException {
        ps.setPoolable(bln);
    }

    public boolean isPoolable() throws SQLException {
        return ps.isPoolable();
    }

    public <T> T unwrap(Class<T> type) throws SQLException {
        return ps.unwrap(type);
    }

    public boolean isWrapperFor(Class<?> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}