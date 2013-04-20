package com.screenscraper.datamanager;

import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.skeleton.BasicSchemaAttr;
import java.io.UnsupportedEncodingException;

import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A DataNode is a temporary storage unit for a chunk of related data.  The data it holds
 * is fairly equivalent to a single row of a database table or a node of an xml file.  It also
 * contains a AbstractSchema which describes how the data it holds is related to parent and child DataNodes.
 *
 * For the most part you creation of DataNodes and insertion of data into them
 * should be dealt with by the DataManager via the addData methods, but manual creation
 * is also permitted.  A manually created DataNode is added to the DataManager via
 * the addDataNode method of the DataManager.
 * @author ryan
 */
public class DataNode implements Comparable {

    private RelationalSchema schema;
    private Map<String, DataObject> values = new TreeMap<String, DataObject>();
    private SortedSet<DataNode> parents;
    private SortedSet<DataNode> children;
    private boolean committed = false;
    private boolean inserted = false;
    private boolean aborted = false;
    private long createTime;
    private boolean updateable;
    private boolean mergeable;
    private Log log = LogFactory.getLog(DataNode.class);

    /**
     * Generates a new node based on the schema.  Fills in any empty data points
     * with the AbstractSchema's declared 'defaultValue' attribute, if any.
     * @param templateSchema	AbstractSchema for which this DataNode instantiates
     */
    public DataNode(RelationalSchema templateSchema) {
        createTime = System.nanoTime();
        initialize(templateSchema);
    }

    private void initialize(RelationalSchema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("schema is null");
        }

        this.schema = schema;
        updateable = false;
        mergeable = false;

        // Load the default values from the AbstractSchema
        //Adding default values now messes up lookups and time values
        //values.putAll(schema.getDefaultValues());

        children = new TreeSet<DataNode>();
        parents = new TreeSet<DataNode>();
    }

    public Map<String, DataObject> getValues() {
        return new HashMap(values);
    }

    /**
     * returns all the descendents of the current node with the name table
     * @param table
     * @return a set of DataNode containing children
     */
    public SortedSet<DataNode> getChildren(String table) {
        SortedSet<DataNode> found = new TreeSet<DataNode>();
        Set<DataNode> visited = new HashSet<DataNode>();
        getChildren(table, this, found, visited);
        return found;
    }

    private void getChildren(String table, DataNode currentNode, SortedSet<DataNode> found, Set<DataNode> visited) {
        if (visited.contains(currentNode)) {
            return;
        }
        visited.add(currentNode);
        if (currentNode.getSchema().getName().equals(table)) {
            found.add(currentNode);
        }
        for (DataNode child : currentNode.children) {
            getChildren(table, child, found, visited);
        }
    }

    public DataObject getValue(String column) {
        for (String key : values.keySet()) {
            if (key.toLowerCase().equals(column.toLowerCase())) {
                return values.get(key);
            }
        }
        return null;
    }

    /**
     * adds a child to the node
     * @param child
     */
    public synchronized void addChild(DataNode child) {
        children.add(child);
        child.parents.add(this);
    }

    public void addParent(DataNode n) {
        n.addChild(this);
    }

    public synchronized void removeParent(DataNode n) {
        parents.remove(n);
        n.children.remove(this);
    }

    public synchronized void removeChild(DataNode n) {
        children.remove(n);
        n.parents.remove(this);
    }

    public Set<DataNode> getParents() {
        return parents;
    }

    public Set<DataNode> getChildren() {
        return children;
    }

    /**
     * Adds a map of data to the schema, if a key in the map matches up with
     * a key in the node's schema it will store the data, otherwise it will ignore it
     *
     * @param	candidateData Map containing name value pairs to try to add to the schema
     */
    public synchronized void addData(Map<String, ? extends Object> candidateData) throws UnsupportedEncodingException {
        if (candidateData == null) {
            return;
        }
        for (Entry<String, ? extends Object> e : candidateData.entrySet()) {
            String column = matchCase(e.getKey(), this.schema);
            Object o = e.getValue();
            DataObject dt = schema.convertToDataObject(column, o);
            if (dt != null) {
                log.debug("Saving \"" + dt.getObject() + "\" into " + column + " as " + dt.getType());
                values.put(column, dt);
            }
        }
    }

    public void clearData() {
        this.values = new HashMap<String, DataObject>();
    }

    public RelationalSchema getSchema() {
        return schema;
    }

    /**
     * Same as other method which takes a Map, except that this one takes only
     * a single name-value pair
     *
     * @param columnName	Candidate column name to be added to AbstractSchema's data
     * @param value			Candidate valud to be added
     * @throws UnsupportedEncodingException
     */
    public void addData(String columnName, Object value) throws UnsupportedEncodingException {
        DataObject dt = schema.convertToDataObject(columnName, value);
        log.debug("Saving \"" + dt.getObject() + "\" into " + columnName + " as " + dt.getType());
        values.put(matchCase(columnName, this.schema), dt);
    }

    public void addData(String columnName, DataObject d) {
        values.put(matchCase(columnName, this.schema), d);
    }

    public String matchCase(String inputCase, RelationalSchema s) {
        for (String schemaCase : s.getColumns()) {
            if (inputCase.equalsIgnoreCase(schemaCase)) {
                return schemaCase;
            }
        }
        return inputCase;
    }

    // <editor-fold defaultstate="collapsed" desc="comment">
    @Override
    public String toString() {
        StringBuilder value = new StringBuilder("NAME: ").append(schema.getName()).append("\nVALUES\n");
        for (String key : values.keySet()) {
            value.append(key).append(":").append(values.get(key)).append("\n");
        }
        return value.toString();
    }// </editor-fold>

    /**
     * if the writer supports it this will update the values instead
     * of rewriting them, for example on a database it will call an UPDATE on a
     * write that returns a duplicate.
     */
    public void setUpdateable() {
        this.updateable = true;
    }

    /**
     *
     * @return true if update is set for the DataNode
     */
    public boolean isUpdateable() {
        return updateable;
    }

    /**
     * Set the DataNode as being mergeable
     */
    public void setMergeable() {
        this.mergeable = true;
    }

    /**
     * returns whether the DataNode is mergeable
     * @return true if DataNode is mergable
     */
    public boolean isMergeable() {
        return mergeable;
    }

    /**
     * Sets that this DataNode has been inserted into the Database
     */
    public void setInserted() {
        inserted = true;
    }

    /**
     * returns whether or not the DataNode was written to the database
     * @return true if the DataNode was inserted
     */
    public boolean getInserted() {
        return inserted;
    }

    public boolean isAborted() {
        return aborted;
    }

    public void abortWrite() {
        this.aborted = true;
    }

    public void setCommitted() {
        committed = true;
    }

    public boolean isCommitted() {
        return committed;
    }

    public Map<String, Object> getObjectMap() {
        Map<String, DataObject> vals = getValues();
        Map<String, Object> oVals = new TreeMap<String, Object>();
        for (Entry<String, DataObject> entry : vals.entrySet()) {
            oVals.put(entry.getKey(), entry.getValue().getObject());
        }
        return oVals;
    }

    /**
     * This is used to compare DataNode by name/commit time so we can sort by the
     * order they were created when they are in the tree.  That way newer data will
     * always overwrite old data.
     * @param o
     * @return Ordering based on Schema name -> commitTime
     */
    public int compareTo(Object o) {
        DataNode n;
        if (o instanceof DataNode) {
            n = (DataNode) o;
        } else {
            throw new IllegalArgumentException("Comparable object must be DataNode");
        }
        if (n.getSchema() == getSchema()) {
            long timeDiff = createTime - n.createTime;
            if (timeDiff > 0) {
                return 1;
            } else if (timeDiff < 0) {
                return -1;
            } else {
                return 0;
            }
        } else {
            return getSchema().getName().compareTo(n.getSchema().getName());
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (this.schema != null ? this.schema.hashCode() : 0);
        hash = 31 * hash + (int) (this.createTime ^ (this.createTime >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DataNode) {
            DataNode n = (DataNode) o;
            if (!getSchema().equals(n.getSchema())) {
                return (n.createTime == createTime);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}