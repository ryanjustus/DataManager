/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager;

import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataObject.ObjectType;
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
public class DataObjectTest {

    public DataObjectTest() {
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
     * Test of getType method, of class DataObject.
     */
    @Test
    public void testFromStringToString() {
        System.out.println("getType String to String");
        String value = "test";
        DataObject instance = new DataObject(value, ObjectType.fromObject(value));
        ObjectType expResult = ObjectType.String;
        ObjectType result = instance.getType();
        assertEquals(expResult, result);

        Object expObject = instance.getObject();
        assertEquals(expObject, value);
    }

    /**
     * Test of getType method, of class DataObject.
     */
    @Test
    public void testFromIntToInteger() {
        System.out.println("getType Int to Integer");

        int value = 1;
        ObjectType expResultType = ObjectType.Integer;

        //create instance
        DataObject instance = new DataObject(value, ObjectType.fromObject(value));

         //assert that the type is Integer
        ObjectType resultType = instance.getType();
        assertEquals(expResultType, resultType);
        
         //assert that the value returned is what we put in
        assertEquals(instance.getObject(), value);
    }


       /**
     * Test of getType method, of class DataObject.
     */
    @Test
    public void testFromIntegerToInteger() {
        System.out.println("getType Integer to Integer");
        Integer value = new Integer(1);
        ObjectType expResult = ObjectType.Integer;

        //create instance
        DataObject instance = new DataObject(value, ObjectType.fromObject(value));

        //assert that Object type is Integer
        ObjectType result = instance.getType();
        assertEquals(expResult, result);

        //assert that Object value is what we put in
        assertEquals(instance.getObject(), value);
    }

     /**
     * Test of getType method, of class DataObject.
     */
    @Test
    public void testFromStringToInteger() {
        System.out.println("getType String to Integer");

       //create instance for Integer string
        String value = "1";
        DataObject instance = new DataObject(value, ObjectType.Integer);
        assertEquals(instance.getType(), ObjectType.Integer);
        assertEquals(instance.getObject(),1);

        //create instance from float string
        value = "1.5";
        instance = new DataObject(value, ObjectType.Integer);
        assertEquals(instance.getType(), ObjectType.Integer);
        assertEquals(instance.getObject(),1);

        try{
            value = "bad";
            instance = new DataObject(value, ObjectType.Integer);
            
            //shouldn't be able to get here
            assertTrue(false);

        }catch(NumberFormatException e){
            assertTrue(true);
        }

        value = "good 1.5";
        instance = new DataObject(value, ObjectType.Integer);
        assertEquals(instance.getType(), ObjectType.Integer);
        assertEquals(instance.getObject(),1);
        
        value = "-good -10092.9";
        instance = new DataObject(value, ObjectType.Float);
        assertEquals(instance.getType(),ObjectType.Float);
        assertEquals(instance.getObject(),-10092.9F);
    }

        /**
     * Test of getType method, of class DataObject.
     */
    @Test
    public void testFromNumberToInteger() {
        System.out.println("getType String to Integer");

       //create instance for Integer string
        java.math.BigDecimal valBd = new java.math.BigDecimal(100.5);
        DataObject instance = new DataObject(valBd, ObjectType.Integer);
        assertEquals(instance.getType(), ObjectType.Integer);
        assertEquals(instance.getObject(),100);

        //create instance from float string
        float valFloat = 100.5f;
        instance = new DataObject(valFloat, ObjectType.Integer);
        assertEquals(instance.getType(), ObjectType.Integer);
        assertEquals(instance.getObject(),100);

     }

 
    /**
     * Test of toString method, of class DataObject.
     */
    @Test
    public void testToString() {
        System.out.println("toString");
        String input = "test";
        DataObject instance = new DataObject(input,DataObject.ObjectType.String);
        String result = instance.toString();
    }

}