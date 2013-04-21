/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.sql;

import java.util.Map.Entry;
import java.io.UnsupportedEncodingException;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.DataManagerEvent;
import com.screenscraper.datamanager.DataManagerEventListener;
import com.screenscraper.datamanager.DataManagerEventSource.EventFireTime;
import com.screenscraper.datamanager.sql.SqlDataManager;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import java.util.Map;
import java.util.HashMap;
import com.screenscraper.datamanager.RelationalSchema;
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
public class EventFireTest {

   
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
    public EventFireTest() {

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
        
        
        log.info("Starting test suite for DuplicateFilter");
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
    public void testBuildSchemas() {
        Connection con = null;
        try{
            con=ds.getConnection();
            System.out.println(ds.getDefaultCatalog());
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "data VARCHAR(100) NOT NULL, " +
                    "cur_timestamp TIMESTAMP, " +
                    "status TINYINT" +
                    ")";

            stmt.execute(createMain);

            String createChild = "CREATE TABLE test_child (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "main_id INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "INDEX(main_id), " +
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
        Map<String,DataManagerEventListener> eventListeners = new HashMap<String,DataManagerEventListener>();
        Map<String,Object> vars= new HashMap<String,Object>();
        vars.put("ignore", "val");
        vars.put("data", "important");
        dm.buildSchemas();   
        addVariablesOnCommit(dm,eventListeners,true,vars);             
 //       dm.addData("test_main","status",1);
        dm.commit("test_main");
        dm.flush();  
        
        //Check to see that "important" was saved into test_main.data
        con = null;
        try{
            con=dm.getConnection();
            PreparedStatement ps = con.prepareStatement("SELECT * FROM test_main");
            ResultSet rs = ps.executeQuery();
            int count=0;
            while(rs.next()){
                count++;
                assertEquals(rs.getString("data"),"important");
            }
            assertTrue(count>0);
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        dm.close();       
    }  
    
    public void addVariablesOnCommit(SqlDataManager dm, Map<String,DataManagerEventListener> eventListeners, boolean val,final Map<String,Object> vars){
        if(val){
            Set<RelationalSchema> ss = dm.getDatabaseSchema().getRelationalSchemas();          
            for(RelationalSchema s: ss){
                String sname = s.getName();
                if(eventListeners.containsKey(sname)){
                    continue;
                }
                DataManagerEventListener e = s.addEventListener(EventFireTime.onCommit, new DataManagerEventListener(){
                    @Override
                    public void handleEvent(DataManagerEvent event) {
                        try {
                            DataNode n = event.getDataNode();
                            log.debug("adding session variables to "+ n.getSchema().getName());
                            n.addData(vars);
                        } catch (UnsupportedEncodingException ex) {
                            log.error("error adding session variables", ex);
                        }
                    }
                });
                eventListeners.put(sname, e);
            }            
        }else{
            for(Entry<String,DataManagerEventListener> entry : eventListeners.entrySet()){
                RelationalSchema s = dm.getDatabaseSchema().getRelationalSchema(entry.getKey());
                s.removeEventListener(EventFireTime.onCommit, entry.getValue());                
            }            
        }    
    }
}