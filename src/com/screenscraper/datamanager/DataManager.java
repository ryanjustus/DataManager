/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author ryan
 */
public interface DataManager { 

    public void buildSchemas();
    public Set<String> getColumns(String table);
    public RelationalSchema getSchema(String table);
    public boolean hasSchema(String table);
    public void addData(String schema, Map<String,Object>data);
    public void addData(String schema, String columName, Object value);
    public boolean commit(String schema);
    public boolean checkDataNode(String schema);
    public DataNode getCurrentDataNode(String schema);
    public DataNode getNewDataNode( String table );
    public void addData(DataNode n);
    public void clearData(String schema);
    public void clearAllData();
    public DataManager setDatabaseSchema(DatabaseSchema s);
    public DatabaseSchema getDatabaseSchema();
    public DataManagerEventListener addDataAssertion(final DataAssertion d);
    public DataManagerEventListener addDataAssertion(final DataAssertion d, EventFireTime when);
    public DataManagerEventListener addEventListener(String schema, EventFireTime when, DataManagerEventListener listener);
    public boolean flush();
    public RootNode getRoot();
    public void close();
}