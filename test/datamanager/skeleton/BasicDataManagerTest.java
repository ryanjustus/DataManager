/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.skeleton;

import com.screenscraper.datamanager.skeleton.BasicDataManager;
import com.screenscraper.datamanager.skeleton.BasicSchema;
import com.screenscraper.datamanager.skeleton.BasicSchemaAttr;
import com.screenscraper.datamanager.SchemaKey;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.HashSet;
import com.screenscraper.datamanager.DataManager;
import com.screenscraper.datamanager.SchemaAttr;
import java.util.HashMap;
import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.RelationalSchema;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ryan
 */
public class BasicDataManagerTest {
    DataManager dm;

    public BasicDataManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        dm = new BasicDataManager();
        RelationalSchema child = new BasicSchema("child");
        HashMap<SchemaAttr, String> attrs = new HashMap<SchemaAttr, String>();
        attrs.put(BasicSchemaAttr.type, DataObject.ObjectType.String.name());
        attrs.put(BasicSchemaAttr.parentKey, "key");
        attrs.put(BasicSchemaAttr.parentSchema, "parent");
        child.addColumn("parentKey", ObjectType.String, attrs);
        child.addColumn("key", ObjectType.String);
        SchemaKey key = new SchemaKey("key", SchemaKey.Type.primary, Arrays.asList("key"));
        child.setPrimaryKey(key);
        
        attrs = new HashMap<SchemaAttr, String>();
        attrs.put(BasicSchemaAttr.type, DataObject.ObjectType.String.name());
        attrs.put(BasicSchemaAttr.defaultValue, "default payload");
        child.addColumn("data", ObjectType.String, attrs);

        dm.getDatabaseSchema().addRelationalSchema(child);
        RelationalSchema parent = new BasicSchema("parent");
        parent.addColumn("key", ObjectType.String);
        parent.setPrimaryKey(key);
        parent.addColumn("data", ObjectType.String);
        dm.getDatabaseSchema().addRelationalSchema(parent);
    }

    @After
    public void tearDown() {
    }


    /**
     * Test of getColumns method, of class BasicDataManager.
     */
    @Test
    public void testGetColumns() {
        System.out.println("getColumns");
        String table = "child";
        Set<String> expResult = new HashSet<String>();
        expResult.add("key");
        expResult.add("parentKey");
        expResult.add("data");
        Set<String> result = dm.getColumns(table);
        assertEquals(expResult, result);
    }

    /**
     * Test of getSchema method, of class BasicDataManager.
     */
    @Test
    public void testGetSchema() {
        System.out.println("getSchema");
        String table = "child";
        RelationalSchema expResult = null;
        RelationalSchema result = dm.getSchema(table);
        assertEquals(result.getName(), table);
    }

    /**
     * Test of addData method, of class BasicDataManager.
     */
    @Test
    public void testAddData() {

        dm.addData("parent", "key", "001P");
        System.out.println("addData");
        Map data = new HashMap();
        data.put("test","junk test");
        data.put("data","test");
        data.put("key", "001");
        dm.addData("child", data);

        DataNode parent = dm.getCurrentDataNode("parent");
        assertEquals(parent.getValue("key").getObject(), "001P");

        DataNode child = dm.getCurrentDataNode("child");
        Map values = child.getObjectMap();
        assertEquals(values.get("data"), data.get("data"));
        assertEquals(values.get("key"), data.get("key"));
        assertFalse(values.keySet().contains("test"));
        
    }

    /**
     * Test of mergeByKeys method, of class BasicDataManager.
     */
    @Test
    public void testMergeByKeys() throws Exception {

    }

    /**
     * Test of commit method, of class BasicDataManager.
     */
    @Test
    public void testCommit() {
        dm.addData("parent", "key", "001P");
        System.out.println("addData");
        Map data = new HashMap();
        data.put("test","junk test");
        data.put("data","test");
        data.put("key", "001");
        dm.addData("child", data);
        DataNode child = dm.getCurrentDataNode("child");
        assertEquals(child.getSchema().getName(), "child");
        dm.commit("child");

        DataNode parent = dm.getCurrentDataNode("parent");
        assertEquals(parent.getValue("key").getObject(), "001P");

        child = dm.getCurrentDataNode("child");
        assertEquals(child,null);
    }

    /**
     * Test of logError method, of class BasicDataManager.
     */
    @Test
    public void testLogError() {
        
    }

    /**
     * Test of logDebug method, of class BasicDataManager.
     */
    @Test
    public void testLogDebug() {
     
    }

    /**
     * Test of commitAll method, of class BasicDataManager.
     */
    @Test
    public void testCommitAll() {
    }

    /**
     * Test of checkDataNode method, of class BasicDataManager.
     */
    @Test
    public void testCheckDataNode() {
        
    }

    /**
     * Test of getMessage method, of class BasicDataManager.
     */
    @Test
    public void testGetMessage() {

    }

    /**
     * Test of rollback method, of class BasicDataManager.
     */
    @Test
    public void testRollback() {

    }

    /**
     * Test of setLoggingLevel method, of class BasicDataManager.
     */
    @Test
    public void testSetLoggingLevel() {

    }

    /**
     * Test of getLoggingLevel method, of class BasicDataManager.
     */
    @Test
    public void testGetLoggingLevel() {

    }

    /**
     * Test of connectTree method, of class BasicDataManager.
     */
    @Test
    public void testConnectTree() {
    }

    /**
     * Test of getNewDataNode method, of class BasicDataManager.
     */
    @Test
    public void testGetNewDataNode() {

    }

    /**
     * Test of addDataNode method, of class BasicDataManager.
     */
    @Test
    public void testAddDataNode() {

    }

    /**
     * Test of addSchema method, of class BasicDataManager.
     */
    @Test
    public void testAddSchema() {

    }

    /**
     * Test of clearAllData method, of class BasicDataManager.
     */
    @Test
    public void testClearAllData() {

    }

    /**
     * Test of getSchemas method, of class BasicDataManager.
     */
    @Test
    public void testGetSchemas() {

    }

    /**
     * Test of addForeignKey method, of class BasicDataManager.
     */
    @Test
    public void testAddForeignKey() {

    }

    /**
     * Test of setSchemas method, of class BasicDataManager.
     */
    @Test
    public void testSetSchemas() {
 
    }

    /**
     * Test of flush method, of class BasicDataManager.
     */
    @Test
    public void testFlush() {

    }

    /**
     * Test of write method, of class BasicDataManager.
     */
    @Test
    public void testWrite() {

    }

    /**
     * Test of getRoot method, of class BasicDataManager.
     */
    @Test
    public void testGetRoot() {

    }

    /**
     * Test of generateUID method, of class BasicDataManager.
     */
    @Test
    public void testGenerateUID() throws Exception {
     
    }

    /**
     * Test of hashData method, of class BasicDataManager.
     */
    @Test
    public void testHashData_SortedMap() {
        SortedMap<String,DataObject> m = new TreeMap();
        m.put("test1", new DataObject(1, ObjectType.Integer));
        m.put("test2", new DataObject("1", ObjectType.String));
        m.put("test3", new DataObject(false, ObjectType.Boolean));
        String hash = BasicDataManager.hashData(m);
        System.out.println("hash: length="+hash.length());
        System.out.println(hash);

        for(int i=0;i<1000;i++){
            m = new TreeMap();
            m.put("test1", new DataObject(i, ObjectType.Integer));
            m.put("test2", new DataObject("10000", ObjectType.String));
            m.put("test3", new DataObject(false, ObjectType.Boolean));
            hash = BasicDataManager.hashData(m);
            System.out.println("****" + hash.length());
            System.out.println(hash);
            assertTrue(hash.length()==31);
        }

    }

    /**
     * Test of hashData method, of class BasicDataManager.
     */
    @Test
    public void testHashData_DataNode_Set() {
        System.out.println("hashData");
        DataNode n = null;
        Set<String> columns = null;
        String expResult = "";
        String result = BasicDataManager.hashData(n, columns);
        assertEquals(expResult, result);
    }

    /**
     * Test of catData method, of class BasicDataManager.
     */
    @Test
    public void testCatData_SortedMap() {
        System.out.println("catData");
        SortedMap<String, DataObject> m = null;
        String expResult = "";
        String result = BasicDataManager.catData(m);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
    }

    /**
     * Test of catData method, of class BasicDataManager.
     */
    @Test
    public void testCatData_DataNode_Set() {
        System.out.println("catData");
        DataNode n = null;
        Set<String> columns = null;
        String expResult = "";
        String result = BasicDataManager.catData(n, columns);
        assertEquals(expResult, result);
    }


    /**
     * Test of close method, of class BasicDataManager.
     */
    @Test
    public void testClose() {
        System.out.println("close");
        BasicDataManager instance = new BasicDataManager();
        instance.close();
    }

}