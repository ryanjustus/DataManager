/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.skeleton;

import com.screenscraper.datamanager.skeleton.BasicSchema;
import com.screenscraper.datamanager.skeleton.BasicSchemaAttr;
import java.util.HashMap;
import java.util.HashSet;
import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataObject.ObjectType;
import com.screenscraper.datamanager.SchemaAttr;
import java.util.Map;
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
public class BasicSchemaTest {

    public BasicSchemaTest() {
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
     * Test of addColumn method, of class BasicSchema.
     */
    @Test
    public void testAddColumn_String_DataObjectObjectType() {
        System.out.println("addColumn");
        String column = "testColumn";
        ObjectType type = ObjectType.Object;
        BasicSchema instance = new BasicSchema("test");
        instance.addColumn(column, type);

        //Test that the column got added
        Set<String> expectedResult = new HashSet();
        expectedResult.add(column);
        Set<String> result = instance.getColumns();
        assertEquals(result, expectedResult);

        //test that the default values are in place
        Map<SchemaAttr, String> resultAttrs = instance.getAttrs(column);
        Map<SchemaAttr, String> expectedAttrs = instance.getDefaultAttrs();
        System.out.println(column + " attrs: " + resultAttrs);
        assertEquals(resultAttrs,expectedAttrs);

    }

    /**
     * Test of addColumn method, of class BasicSchema.
     */
    @Test
    public void testAddColumn_3args() {

        //test with null 3d param, should be same as 2 param
        System.out.println("addColumn");
        String column = "testColumn";
        ObjectType type = ObjectType.Object;
        BasicSchema instance = new BasicSchema("test");
        instance.addColumn(column, type, null);

        //Test that the column got added
        Set<String> expectedResult = new HashSet();
        expectedResult.add(column);
        Set<String> result = instance.getColumns();
        assertEquals(result, expectedResult);

        //test that the default values are in place
        Map<SchemaAttr, String> resultAttrs = instance.getAttrs(column);
        Map<SchemaAttr, String> expectedAttrs = instance.getDefaultAttrs();
        System.out.println(column + " attrs: " + resultAttrs);
        assertEquals(resultAttrs,expectedAttrs);


        //test with empty map 3d param, should be same as 2 param
        System.out.println("addColumn");
        column = "testColumn";
        type = ObjectType.Object;
        instance = new BasicSchema("test");
        instance.addColumn(column, type, new HashMap());

        //Test that the column got added
        expectedResult = new HashSet();
        expectedResult.add(column);
        result = instance.getColumns();
        assertEquals(result, expectedResult);

        //test that the default values are in place
        resultAttrs = instance.getAttrs(column);
        expectedAttrs = instance.getDefaultAttrs();
        System.out.println(column + " attrs: " + resultAttrs);
        assertEquals(resultAttrs,expectedAttrs);


        //test with non-empty map 3d param, should be same as same as 3d param joined with default vals
        System.out.println("addColumn");
        column = "testColumn";
        type = ObjectType.String;
        instance = new BasicSchema("test");
        HashMap<SchemaAttr, String> attrs = new HashMap<SchemaAttr, String>();
        attrs.put(BasicSchemaAttr.type, DataObject.ObjectType.String.name());
        attrs.put(BasicSchemaAttr.defaultValue, "testDefault");
        attrs.put(BasicSchemaAttr.parentKey, "parentKey");
        attrs.put(BasicSchemaAttr.parentSchema, "parentSchema");

        instance.addColumn(column, type, attrs);

        //Test that the column got added
        expectedResult = new HashSet();
        expectedResult.add(column);
        result = instance.getColumns();
        assertEquals(result, expectedResult);

        //test that the default values are in place
        resultAttrs = instance.getAttrs(column);
        expectedAttrs = instance.getDefaultAttrs();
        expectedAttrs.putAll(attrs);
        System.out.println(column + " attrs: " + resultAttrs);
        assertEquals(resultAttrs,expectedAttrs);
    }


    /**
     * Test of setObjectType method, of class BasicSchema.
     */
    @Test
    public void testSetObjectType() {
        System.out.println("setObjectType");
        String column = "testColumn";
        ObjectType type = ObjectType.BigDecimal;
        BasicSchema instance = new BasicSchema("test");
        instance.addDefaultColumn(column);
        instance.setObjectType(column, type);
        ObjectType result = instance.getObjectType(column);
        assertEquals(type,result);
    }

    /**
     * Test of addAttr method, of class BasicSchema.
     */
    @Test
    public void testAttr() {
        System.out.println("test attr");
        String column = "testColumn";
        SchemaAttr attr = BasicSchemaAttr.size;
        String attrVal = "50";
        BasicSchema instance = new BasicSchema("test");
        instance.addAttr(column, attr, attrVal);

        String result = instance.getAttr(column, attr);
        assertEquals(result, attrVal);

        //re-adding to same column with different type
        instance.addColumn(column, ObjectType.String, null);
        result = instance.getAttr(column, attr);
        assertEquals(result, attrVal);


        //adding a different attribute, this shouldn't change the attribute size
        instance.addAttr(column, BasicSchemaAttr.isNullable, BasicSchemaAttr.Option.no.name());
        result = instance.getAttr(column, attr);
        assertEquals(result, attrVal);

    }

    /**
     * Test of addForeignKey method, of class BasicSchema.
     */
    @Test
    public void testForeignKey() {

    }

       /**
     * Test of convertToDataObject method, of class BasicSchema.
     */
    @Test
    public void testConvertToDataObject() {
        System.out.println("convertToDataObject");


        //column doesn't exists, should return null
        String column = "testColumn";
        Object o = "test";
        BasicSchema instance = new BasicSchema("test");
        DataObject expResult = null;
        DataObject result = instance.convertToDataObject(column, o);
        assertEquals(expResult, result);

        //input is null, should return DataObject<null>
        instance.addColumn(column, ObjectType.String);
        result = instance.convertToDataObject(column,  null);
        assertEquals(new DataObject(null, ObjectType.String).getObject(), result.getObject());       
    }

    /**
     * Test of getAutoGeneratedColumns method, of class BasicSchema.
     */
    @Test
    public void testGetAutoGeneratedColumns() {
        System.out.println("getAutoGeneratedColumns");
        BasicSchema instance = new BasicSchema("test");
        Set expResult = new HashSet();
        Set result = instance.getAutoGeneratedColumns();
        assertEquals(expResult, result);
    }

 

    /**
     * Test of getObjectType method, of class BasicSchema.
     */
    @Test
    public void testGetObjectType() {
        System.out.println("getObjectType");
        String column = "testColumn";
        BasicSchema instance = new BasicSchema("test");
        instance.addColumn(column, ObjectType.String);
        ObjectType result = instance.getObjectType(column);
        assertEquals(ObjectType.String, result);
    }

 
    /**
     * Test of getName method, of class BasicSchema.
     */
    @Test
    public void testGetName() {
        System.out.println("getName");
        String name = "test";
        BasicSchema instance = new BasicSchema(name);
        String result = instance.getName();
        assertEquals(name, result);
    }
}