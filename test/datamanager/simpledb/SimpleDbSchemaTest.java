/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.simpledb;

import com.screenscraper.datamanager.simpledb.SimpleDbSchema;
import com.screenscraper.datamanager.simpledb.SimpleDbSchemaBuilder;
import java.util.Map;
import java.util.HashMap;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.SchemaKey;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.simpledb.SimpleDbSchema.SimpleDbSchemaAttrs;
import com.screenscraper.datamanager.simpledb.SimpleDbSchema.SimpleDbSchemaAttrs.Option;
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
public class SimpleDbSchemaTest {

    public SimpleDbSchemaTest() {
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

    /**
     * Test of isDomainSchema method, of class SimpleDbSchema.
     */
    @Test
    public void testSingleValueSchema() {

        SimpleDbSchema parent = new SimpleDbSchema("parent");
        SchemaKey key = new SchemaKey("pk", SchemaKey.Type.primary, Arrays.asList("pk"));
        parent.setPrimaryKey(key);
        SimpleDbSchema child = new SimpleDbSchema("child");
        child.addColumn("value", ObjectType.String);
        child.addRelationship("value", parent, SimpleDbSchemaAttrs.Option.singleValue.toString());
        Set fkTables = new HashSet();
        fkTables.add("parent");
        Set actual = child.getForeignTables();
        System.out.println("actual: "+ actual);
        assertEquals(fkTables,actual);
    }

    /**
     * Test of isDomainSchema method, of class SimpleDbSchema.
     */
    @Test
    public void testRelationships() {
        System.out.println("isDomainSchema");
        SimpleDbSchema instance = new SimpleDbSchema("test");
        System.out.println("*****");
        System.out.println(instance);
        System.out.println("*****");
        boolean result = instance.isDomainSchema();
        //there is no key information so it isn't valid
        assertEquals(true, result);

        //set a subDocument relationship, no longer valid because we don't have
        //an fk relationship set
        instance.addAttr("fk1", SimpleDbSchemaAttrs.subFormat, SimpleDbSchemaAttrs.Option.json.toString());
        System.out.println("*****");
        System.out.println(instance);
        System.out.println("*****");
        result = instance.isSubDocumentSchema();
        assertEquals(false, result);

        //define an fk relationship to make it valid
        Map<String,String> fkMap = new HashMap<String,String>();
        fkMap.put("fk1", "otherColumn");
        SchemaForeignKey fk = new SchemaForeignKey("fk1", "otherTable", fkMap);
        instance.addForeignKey(fk);
        result = instance.isSubDocumentSchema();
        assertEquals(true, result);

        //since we have set a subDocument relationship and no domain relationships
        //it is no longer a domain schema
        result = instance.isDomainSchema();
        assertEquals(false, result);

        //add in a domain relationship
        instance.addAttr("fk2", SimpleDbSchemaAttrs.subFormat, Option.domain.toString());
        instance.addForeignKey("fk2", "otherTable2", "otherColumn2");
        result = instance.isDomainSchema();
        assertEquals(true, result);
    }

    /**
     *
     */
    @Test
    public void testAddRelationships() {
        System.out.println("isDomainSchema");
        SimpleDbSchema instance = new SimpleDbSchema("test");
        SimpleDbSchema parent = new SimpleDbSchema("parent");
        SchemaKey key = new SchemaKey("pk", SchemaKey.Type.primary, Arrays.asList("pk"));
        parent.setPrimaryKey(key);
        instance.addRelationship("fk", parent, "domain");
        assertEquals(true, instance.isDomainSchema());
        
        instance.addRelationship("fk2", parent, "json");
        assertEquals(true, instance.isDomainSchema());
        assertEquals(true, instance.isSubDocumentSchema());
        
        System.out.println("*****");
        System.out.println(instance);
        System.out.println("*****");
        System.out.println("parentSchema");
        System.out.println(parent);

    }


        /**
     *
     */
    @Test
    public void testSetObjectType() {
        System.out.println("isDomainSchema");
        SimpleDbSchema instance = new SimpleDbSchema("test");
        instance.addColumn("test", ObjectType.Object);
        instance.setObjectType("test", ObjectType.String);
        assertEquals(instance.getObjectType("test"), ObjectType.String);
        ObjectType t = SimpleDbSchemaBuilder.typeFromString("timestamp");
        System.out.println("****" + t.toString());
        instance.setObjectType("test", SimpleDbSchemaBuilder.typeFromString("timestamp"));
        assertEquals(instance.getObjectType("test"), ObjectType.Timestamp);
    }

}