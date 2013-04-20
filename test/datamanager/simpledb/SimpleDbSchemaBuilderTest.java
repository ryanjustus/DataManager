/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.simpledb;

import com.screenscraper.datamanager.simpledb.SimpleDbSchemaBuilder;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import com.screenscraper.datamanager.RelationalSchema;
import java.util.HashSet;
import com.screenscraper.datamanager.DatabaseSchema;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.xml.sax.SAXException;

/**
 *
 * @author ryan
 */
public class SimpleDbSchemaBuilderTest {

    public SimpleDbSchemaBuilderTest() {
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
     * Test of getSchemas method, of class SimpleDbSchemaBuilder.
     */
    @Test
    public void testGetSchemas() throws ParserConfigurationException, SAXException, IOException {
        System.out.println("getSchemas");
        File f = new File("schema_sample.xml");
        SimpleDbSchemaBuilder instance = new SimpleDbSchemaBuilder(f);
        Set expResult = new HashSet();
        Set<RelationalSchema> result = instance.getSchemas().getRelationalSchemas();
        for(RelationalSchema s: result){
            System.out.println("****");
            System.out.println(s);
            System.out.println("****");
        }

        //assertEquals(expResult, result);
    }

}