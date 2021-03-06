/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.skeleton;

import com.screenscraper.datamanager.skeleton.BasicSchemaAttr;
import com.screenscraper.datamanager.DataObject;
import java.util.HashSet;
import com.screenscraper.datamanager.SchemaAttr;
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
public class BasicSchemaAttrTest {

    public BasicSchemaAttrTest() {
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
     * Test of valueOf method, of class BasicSchemaAttr.
     */
    @Test
    public void testValueOf() {
        System.out.println("valueOf");
        for(BasicSchemaAttr attr: BasicSchemaAttr.values()){
            String name = attr.toString();
            BasicSchemaAttr result = BasicSchemaAttr.valueOf(name);
            assertEquals(attr, result);
        }
    }


      /**
     * Test of valueOf method, of class BasicSchemaAttr.
     */
    @Test
    public void testFromString() {
        System.out.println("fromString");
        for(BasicSchemaAttr attr: BasicSchemaAttr.values()){
            String name = attr.toString();
            SchemaAttr result = BasicSchemaAttr.fromString(name);
            assertEquals(attr, result);
        }
    }

    /**
     * Test of getOptions method, of class BasicSchemaAttr.
     */
    @Test
    public void testGetOptions() {
        System.out.println("getOptions");

        //test autoGenerated
        BasicSchemaAttr instance = BasicSchemaAttr.autoGenerated;
        Set<String> expResult = new HashSet<String>();
        expResult.add(BasicSchemaAttr.Option.no.name());
        expResult.add(BasicSchemaAttr.Option.yes.name());
        Set<String> result = instance.getOptions();
        assertTrue(result.equals(expResult));

        instance = BasicSchemaAttr.defaultValue;
        expResult = new HashSet<String>();
        System.out.println(expResult);        
        result = instance.getOptions();
        System.out.println(result);
        assertTrue(result.equals(expResult));

    }

    /**
     * Test of getDefaultOption method, of class BasicSchemaAttr.
     */
    @Test
    public void testGetDefaultOption() {
        System.out.println("getDefaultOption");
        BasicSchemaAttr instance = BasicSchemaAttr.type;
        String expResult = DataObject.ObjectType.Object.name();
        String result = instance.getDefaultOption();
        assertEquals(expResult, result);

        instance = BasicSchemaAttr.autoGenerated;
        expResult = BasicSchemaAttr.Option.no.name();
        result = instance.getDefaultOption();
        assertEquals(expResult, result);


    }

    /**
     * Test of toString method, of class BasicSchemaAttr.
     */
    @Test
    public void testToString() {
        System.out.println("toString");
        BasicSchemaAttr instance = BasicSchemaAttr.isNullable;
        String expResult = "isNullable";
        String result = instance.toString();
        assertEquals(expResult, result);

        System.out.println("toString");
        instance = BasicSchemaAttr.type;
        expResult = "type";
        result=instance.toString();
        assertEquals(expResult, result);
    }
}