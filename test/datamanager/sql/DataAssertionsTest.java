/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.sql;

import java.util.Map;
import java.util.HashMap;
import com.screenscraper.datamanager.DataManagerEventListener;
import com.screenscraper.datamanager.DataManagerEvent;
import java.util.Set;
import com.screenscraper.datamanager.DataAssertion;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.sql.SqlDataManager;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.commons.dbcp.BasicDataSource;
import java.sql.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * This test requires MySql installed. Change connections settings according to 
 * your environment below.
 * Tested using MySql Community Edition 5.5.11 64 bit.
 * 
 * Older versions may need the storage engine set to InnoDb (or another engine 
 * that supports foreign key constraints) for test to work.  Alternatively you 
 * can manually specify foreign key constraints that will be applied at the client
 * level using Schema.addForeignKey(SchemaForeignKey fk)
 * NOTE: It will create and use a database named DataManagerTest.
 * @author ryan
 */
public class DataAssertionsTest {   
     /**
     * Modify settings for your database installation
     */
    static final String dbUser = "root",
            dbPass="",
            database="DataManagerTest",
            dbUrl = "jdbc:mysql://127.0.0.1:3306/",
            driver = "com.mysql.jdbc.Driver";
    static BasicDataSource ds;
    private static final Logger log = Logger.getLogger(MySql.class);
    public DataAssertionsTest() {

    }

    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException{
        BasicConfigurator.configure();
        // This will load the MySQL driver, each DB has its own driver
        Class.forName(driver);
        // Setup the connection with the DB
        Connection conn=null;
        try{
            conn = DriverManager.getConnection(dbUrl+"?user="+dbUser+"&password="+dbPass);
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE DATABASE IF NOT EXISTS "+ database);
        }catch(SQLException e){
            log.error("unable to establish connection", e);
        }finally{
            if(conn!=null)
                try{conn.close();}catch(SQLException e){}
        }
    }

    @AfterClass
    public static void tearDownClass(){
        try{ds.close();}catch(Exception e){
            e.printStackTrace(System.err);
        }
    }

    @Before
    public void setUp() {
        Connection con = null;
        try{
            ds = new BasicDataSource();
            ds.setDriverClassName( driver);
            ds.setUsername( dbUser );
            ds.setPassword( dbPass );
            ds.setUrl(dbUrl+database );
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            
            stmt.execute("CREATE DATABASE IF NOT EXISTS "+database);
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            DatabaseMetaData meta = con.getMetaData();
            ResultSet rs = meta.getTables(null, null, null, new String[]{"TABLE"});
            while(rs.next()){
                stmt.execute("DROP TABLE " + rs.getString("TABLE_NAME"));
            }
            stmt.execute("SET FOREIGN_KEY_CHECKS=1");
        }catch(SQLException e){
            e.printStackTrace(System.out);
            assertTrue(false);
        }finally{
            try{con.close();}catch(Exception e){
                e.printStackTrace(System.err);
            }
        }
    }

    @After
    public void tearDown() {
    }

     /**
     * Test parsing out of the schemas.
      * Create two tables with a one-to-many relationship, test_main and test_child
     */
    
    @Test
    public void testNoAutoIncrementError() {
        Connection con = null;
        try{
            con=ds.getConnection();
            System.out.println(ds.getDefaultCatalog());
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100) " +
                        ")";
            stmt.execute(createMain);
            
            String createChild = "CREATE TABLE test_child (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "main_id INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "FOREIGN KEY (main_id) REFERENCES test_main(id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    ")";
            stmt.execute(createChild);
           
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }

        SqlDataManager dm = new SqlDataManager(ds);        
        dm.buildSchemas();        
        
        DataAssertion childCountAssertion = new DataAssertion(){

            public boolean validateData(DataNode n) {
                Set<DataNode> children = n.getChildren("test_child");
                return children.size()>1;                
            }

            public void onFail() {
                log.error("insufficient children");
                assertTrue(false);
            }

            public void onSuccess() {
                log.info("test_main children check succeeded");
                
            }

            public String getSchema() {
                return "test_main";
            }            
        };
        dm.addDataAssertion(childCountAssertion); 
        System.out.println("add data");
        //Do initial insert using case insentivity for tables and columns
        Map data = new HashMap();
        data.put("junk", "shouldn't add");
        data.put("datA", "payload");
        dm.addData("tEst_main", data);

        Map cData = new HashMap();
        cData.put("data", "child payload");
        dm.addData("tEst_child", cData);
        dm.commit("test_cHild");
        
        cData.put("data", "child payload 2");
        dm.addData("tEst_child", cData);
        dm.commit("test_cHild");
        
        dm.commit("test_mAin");
        dm.flush();        
        /**
         * THIS IS LOGGING AN EXCEPTION ON getGeneratedKeys WHEN THERE ARE NO AI KEYS
         * The exception doesn't hurt anything but pollutes the log and should be avoided
         */
        dm.flush();
        dm.close();
    }   

}