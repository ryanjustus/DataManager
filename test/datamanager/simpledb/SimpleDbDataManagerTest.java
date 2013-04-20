/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.simpledb;

import com.screenscraper.datamanager.simpledb.SimpleDbSchema;
import com.screenscraper.datamanager.simpledb.SimpleDbDataManager;
import java.util.Arrays;
import com.screenscraper.datamanager.SchemaKey;
import java.util.HashSet;
import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.simpledb.SimpleDbSchema.SimpleDbSchemaAttrs;
import com.screenscraper.datamanager.RelationalSchema;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.screenscraper.datamanager.DataWriter;
import java.util.Set;
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
public class SimpleDbDataManagerTest {

    public SimpleDbDataManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testSchemas(){
        SimpleDbDataManager dm = new SimpleDbDataManager("AKIAJKWGLQOU5WBH3WVA","jFSiB681MFvCHcvY1r3Ej3aFGbaF9AFeA7d6GBvb","schema_sample.xml");
        SimpleDbSchema parent = new SimpleDbSchema("parent");
        SchemaKey key = new SchemaKey("pk", SchemaKey.Type.primary, Arrays.asList("pk"));
        parent.setPrimaryKey(key);
        dm.getDatabaseSchema().addRelationalSchema(parent);
        SimpleDbSchema child = new SimpleDbSchema("child");
        child.addColumn("value", ObjectType.String);
        child.addRelationship("value", parent, SimpleDbSchemaAttrs.Option.singleValue.toString());
        dm.getDatabaseSchema().addRelationalSchema(child);
        System.out.println("domains in model: ");
        System.out.println(dm.getDomainsInModel());
        Set domains = new HashSet();
        domains.add("parent");
        assertEquals(domains, dm.getDomainsInModel());

    }

    /**
     * Test of flush method, of class SimpleDbDataManager.
     */
    @Test
    public void testFlush() {

        System.out.println("flush");
        SimpleDbDataManager dm = new SimpleDbDataManager("AKIAJKWGLQOU5WBH3WVA","jFSiB681MFvCHcvY1r3Ej3aFGbaF9AFeA7d6GBvb","schema_sample.xml");
        dm.buildSchemas();
        RelationalSchema s = dm.getSchema("sub_specialty");
    }
}