/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.sql;

import com.screenscraper.datamanager.DataAssertion;
import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.sql.SqlDataManager;
import com.screenscraper.datamanager.SchemaForeignKey;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import java.util.Iterator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.screenscraper.datamanager.DataObject;
import java.util.Map;
import java.util.HashMap;
import com.screenscraper.datamanager.RelationalSchema;
import java.util.HashSet;
import java.util.Arrays;
import org.apache.commons.dbcp.BasicDataSource;
import java.sql.*;
import java.util.Set;
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
public class MySqlForeignKeyTest {   
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
    public MySqlForeignKeyTest() {

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
        try{
            ds.close();
        }catch(SQLException e){}
    }

     /**
     * Test aborting a write before it is written
     */
    
    @Test
    public void testManyToMany() {
        Connection con = null;
        try{
            con=ds.getConnection();
            System.out.println(ds.getDefaultCatalog());
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main1 (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "data VARCHAR(100) " +
                    ")";

            stmt.execute(createMain);

            String createMain2 = "CREATE TABLE test_main2 (" +
                    "id INT NOT NULL PRIMARY KEY, " +
                    "data VARCHAR(100) " +
                    ")";
            stmt.execute(createMain2);
            
            String createManyToMany = "CREATE TABLE test_many_to_many (" +
                    "id1 INT NOT NULL, " +
                    "id2 INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "PRIMARY KEY(id1,id2), " +
                    "FOREIGN KEY (id1) REFERENCES test_main1(id), " +
                    "FOREIGN KEY (id2) REFERENCES test_main2(id) " +
                    ")";
            stmt.execute(createManyToMany);
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        
        
        SqlDataManager dm = new SqlDataManager(ds);  
        dm.buildSchemas();       
            
        dm.addData("test_many_to_many","id2","100");        
        dm.addData("test_main1","data","data main 1"); 
        dm.addData("test_main2","id","101");
        dm.commit("test_many_to_many");
        dm.addData("test_main2","data","data child 1");
        dm.commit("test_main2");
        dm.commit("test_main1");        
        dm.flush();
        dm.close();
    }
    
    @Test
    public void testCompositeFk() {
        Connection con = null;
        try{
            con=ds.getConnection();
            System.out.println(ds.getDefaultCatalog());
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main1 (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "data VARCHAR(100) " +
                    ")";

            stmt.execute(createMain);

            String createMain2 = "CREATE TABLE test_main2 (" +
                    "id INT NOT NULL PRIMARY KEY, " +
                    "data VARCHAR(100) " +
                    ")";
            stmt.execute(createMain2);
            
            String createManyToMany = "CREATE TABLE test_many_to_many (" +
                    "id1 INT NOT NULL, " +
                    "id2 INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "PRIMARY KEY(id1,id2), " +
                    "FOREIGN KEY (id1) REFERENCES test_main1(id), " +
                    "FOREIGN KEY (id2) REFERENCES test_main2(id) " +
                    ")";
            stmt.execute(createManyToMany);
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        
        
        SqlDataManager dm = new SqlDataManager(ds);  
        dm.buildSchemas();       
            
        dm.addData("test_many_to_many","id2","100");        
        dm.addData("test_main1","data","data main 1"); 
        dm.addData("test_main2","id","101");
        dm.commit("test_many_to_many");
        dm.addData("test_main2","data","data child 1");
        dm.commit("test_main2");
        dm.commit("test_main1");        
        dm.flush();
        dm.close();
    }

}