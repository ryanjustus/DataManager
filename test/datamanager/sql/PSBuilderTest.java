/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.sql;

import com.screenscraper.datamanager.sql.SqlDataManager;
import com.screenscraper.datamanager.sql.util.DmPreparedStatement;
import com.screenscraper.datamanager.sql.util.PSBuilder;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.util.Arrays;
import com.screenscraper.datamanager.SchemaKey;
import java.util.Map;
import java.util.HashMap;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.dbcp.BasicDataSource;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.sql.SqlDatabaseSchema.DatabaseSchemaAttr;
import java.sql.Connection;
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
public class PSBuilderTest {

    static SqlDataManager dm;
    public PSBuilderTest() {
    }

     @BeforeClass
    public static void setUpClass() throws Exception {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName( "com.mysql.jdbc.Driver" );
        ds.setUsername( "root" );
        ds.setPassword( "" );
        ds.setUrl( "jdbc:mysql://127.0.0.1:3306/test" );

        Connection con = null;
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            stmt.execute("DROP TABLE IF EXISTS test_main,test_child");
            stmt.execute("SET FOREIGN_KEY_CHECKS=1");

            String createMain = "CREATE TABLE test_main (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "data VARCHAR(100), " +
                    "cur_timestamp TIMESTAMP, " +
                    "status TINYINT" +
                    ") TYPE=innodb";

            stmt.execute(createMain);

            String createChild = "CREATE TABLE test_child (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "main_id INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "INDEX(main_id), " +
                    "FOREIGN KEY (main_id) REFERENCES test_main(id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    ") TYPE=innodb";
            stmt.execute(createChild);
            stmt.close();
        }catch(SQLException e){
            System.err.println(e);
        }finally{
            try{con.close();}catch(Exception e){}
        }
        dm = new SqlDataManager(ds);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
       Connection con = null;
       try{
           con = dm.getConnection();
           PreparedStatement ps = con.prepareStatement("TRUNCATE TABLE test_child");
           ps.execute();
           ps = con.prepareStatement("TRUNCATE TABLE test_main");
           ps.execute();
       }catch(SQLException e){
           e.printStackTrace();
       } finally {
           try{con.close();}catch(SQLException e){}
       }

    }

    @After
    public void tearDown() {
    }

      /**
     * Test of getSelect method, of class PSBuilder.
     */
    @Test
    public void testGetSelect() throws Exception {
        dm.buildSchemas();
        System.out.println("getSelect");
        Connection con = null;
        try{
            //add some data
            for(int i=0;i<10;i++){
                Map data = new HashMap();
                data.put("data", "data"+i);
                data.put("status", i%2);
                dm.addData("test_main", data);
                dm.commit("test_main");
            }
            dm.flush();

            con= dm.getConnection();

            DataNode n = new DataNode(dm.getSchema("test_main"));
            //add a fake index to test lookup
            SchemaKey index = new SchemaKey("test_key", SchemaKey.Type.unique, Arrays.asList("data","status"));
            n.getSchema().addIndex(index);
            n.addData("data", "data1");
            n.addData("status", 1);
            String vendor = dm.getDatabaseSchema().getAttr(DatabaseSchemaAttr.Vendor);
            String identifierQuote = dm.getDatabaseSchema().getAttr(DatabaseSchemaAttr.DatabaseIdentifierQuote);
            DmPreparedStatement ps = PSBuilder.getSelect(vendor,con, n, identifierQuote);
            String query = ps.getSql();
            System.out.println(query);
            System.out.println(ps.getData());
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            while(rs.next()){
                for(int i=1;i<=meta.getColumnCount();i++){
                    System.out.println(meta.getColumnName(i)+"="+rs.getString(i));
                }
            }

            
            // TODO review the generated test code and remove the default call to fail.
            fail("The test case is a prototype.");
        }finally{
            con.close();
        }
    }

    /**
     * Test of getUpdate method, of class PSBuilder.
     */
    @Test
    public void testGetUpdate() throws Exception {
        System.out.println("getUpdate");
        Connection con = null;
        DataNode n = null;
        String quote = "";
        DmPreparedStatement expResult = null;
        DmPreparedStatement result = PSBuilder.getUpdate(con, n, quote);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getInsert method, of class PSBuilder.
     */
    @Test
    public void testGetInsert() throws Exception {
        System.out.println("getInsert");
        Connection con = null;
        DataNode n = null;
        String quote = "";
        boolean autoincrement = false;
        DmPreparedStatement expResult = null;
        DmPreparedStatement result = PSBuilder.getInsert(con, n, quote, autoincrement);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}