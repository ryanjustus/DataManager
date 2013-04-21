/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datamanager.sql;

import java.sql.PreparedStatement;
import com.screenscraper.datamanager.sql.SqlDataManager;
import com.screenscraper.datamanager.sql.SqlDuplicateFilter;
import java.sql.DriverManager;
import com.screenscraper.datamanager.sql.util.DmPreparedStatement;
import java.util.Arrays;
import com.screenscraper.datamanager.DatabaseSchema;
import com.screenscraper.datamanager.sql.util.SqlLookup;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import com.screenscraper.datamanager.sql.util.QueryUtils;
import java.util.Map;
import java.util.HashMap;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import org.apache.commons.dbcp.BasicDataSource;
import java.sql.Connection;
import org.apache.log4j.Level;
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
public class MySqlDuplicateFilter {

    public MySqlDuplicateFilter() {}
    static BasicDataSource ds;
    private static Logger log = Logger.getLogger(MySqlDuplicateFilter.class);

    

    static String dbUser = "root",
            dbPass="",
            database="DataManagerTest",
            dbUrl = "jdbc:mysql://127.0.0.1:3306/";

    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException{
          log.info("Starting test suite for DuplicateFilter");
                  // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
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
            BasicConfigurator.configure();
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
            ds.setDriverClassName( "com.mysql.jdbc.Driver" );
            ds.setUsername( "root" );
            ds.setPassword( "" );
            ds.setUrl( "jdbc:mysql://127.0.0.1:3306/"+database );
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
     * Test adding data using various cases with a one-to-many relationship
      * This test is to show the SqlLookup working
     */
    @Test
    public void testSqlLookupPositive() {
        //Initial test setup, create table test_main,test_child
        Connection con = null;
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100), " +
                        "keyCol VARCHAR(10) " +
                        ")";

            stmt.execute(createMain);

            String createChild = "CREATE TABLE test_child (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "main_id INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "keyCol VARCHAR(10), " +
                    "FOREIGN KEY (main_id) REFERENCES test_main(id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    ")";
            stmt.execute(createChild);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }
        finally{
            try{
                con.close();
            }catch(Exception e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            dm.buildSchemas();
            //Do initial insert
            for(int i=1;i<=3;i++){
                Map mainData = new HashMap();
                mainData.put("data", "payload " + i);
                mainData.put("keyCol", "key"+i);
                dm.addData("test_main", mainData);
                for(int j=1;j<=3;j++){
                   Map childData = new HashMap();
                   childData.put("data", "payload " + i + "," +j);
                   childData.put("keyCol", "key" + j);
                   dm.addData("test_child", childData);
                   dm.commit("test_child");
                }
                dm.commit("test_main");
            }
            dm.flush();

            //at this point there should be three rows in test_main with data as "payload i" where i is 0-2
            //each row in test_main should have 3 children with data as "payload i" where i is 0-2

            try {
                con = dm.getConnection();
                Statement stmt = con.createStatement();
                int mainId=-1;
                int count=0;
                ResultSet rs = stmt.executeQuery("SELECT * FROM test_main");
                while(rs.next()){
                    mainId=rs.getInt("id");
                    count++;
                    log.debug(QueryUtils.saveRowAsMap(rs));
                    assertTrue(rs.getString("data").equals("payload "+(mainId)));
                }
                //make sure that an insert happened
                assertTrue(count==3);

                rs = stmt.executeQuery("SELECT * FROM test_child");
                count=0;
                while(rs.next()){
                    count++;
                    log.debug(QueryUtils.saveRowAsMap(rs));
                }
                assertTrue(count==9);

                  //Now that we have verified that there is some data in there lets try to do a lookup
                SqlLookup l = new SqlLookup(dm.getDatabaseSchema());
                l.setDuplicateAction(SqlLookup.DISTINCT);
                l.addSelectColumns("test_main", Arrays.asList("id","data"));
                l.addConstraint("test_main", "keyCol", "key1");
                l.addConstraint("test_child", "keyCol", "key1");
                l.addConstraint("test_child", "keyCol", "key2");
                DmPreparedStatement ps = l.getPreparedStatement(con);
                log.debug(ps.getSql());
                log.debug(ps.getData());
                rs = ps.executeQuery();
                count=0;
                while(rs.next()){
                    log.debug("Match found");
                    log.debug(QueryUtils.saveRowAsMap(rs));
                    count++;
                    assertEquals(rs.getInt("id"),1);
                }
                assertEquals(count,1);

            } catch (SQLException ex) {                
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }

       
        }catch(Exception e){
            log.error("SQL Error", e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
    
         /**
     * Test adding data using various cases with a one-to-many relationship
      * This test is to show the SqlLookup working
     */
    @Test
    public void testUpdateNullUniqueKey() {
        //Initial test setup, create table test_main,test_child
        Connection con = null;
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100), " +
                        "keyCol VARCHAR(10), " +
                        "UNIQUE INDEX `uk` (`keyCol`) USING HASH "+
                        ")";

            stmt.execute(createMain);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{
                con.close();
            }catch(Exception e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            dm.buildSchemas();
            dm.setGlobalMergeEnabled(true);
            dm.setGlobalUpdateEnabled(true);
            
            dm.addData("test_main", "data","test");
            dm.commit("test_main");
            
            dm.addData("test_main","data", "test2");
            dm.commit("test_main");
            dm.flush(); 
            
            dm.addData("test_main", "data","test3");
            dm.addData("test_main","keyCol","key1");
            dm.commit("test_main");
            
            dm.addData("test_main","data", "test4");
            dm.addData("test_main","keyCol","key1");
            dm.commit("test_main");
            dm.flush(); 
            
            con = null;
            try{
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT count(*) as c FROM test_main");
                ResultSet rs = ps.executeQuery();
                while(rs.next()){
                    assertEquals(rs.getInt("c"),3);
                }               
                
            }finally{
                try{con.close();}catch(Exception e){}
            }
            
            
            
   
        }catch(Exception e){
            log.error("SQL Error", e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }

    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
     /**
     * Test adding data using various cases with a one-to-many relationship
     * In this test there shouldn't be a match for the find
     */
    @Test
    public void testSqlLookupNegative() {
        //Initial test setup, create table test_main,test_child
        Connection con = null;
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100), " +
                        "keyCol VARCHAR(10) " +
                        ")";

            stmt.execute(createMain);

            String createChild = "CREATE TABLE test_child (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "main_id INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "keyCol VARCHAR(10), " +
                    "FOREIGN KEY (main_id) REFERENCES test_main(id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    ")";
            stmt.execute(createChild);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            dm.buildSchemas();
            //Do initial insert
            for(int i=1;i<=3;i++){
                Map mainData = new HashMap();
                mainData.put("data", "payload " + i);
                mainData.put("keyCol", "key"+i);
                dm.addData("test_main", mainData);
                for(int j=1;j<=3;j++){
                   Map childData = new HashMap();
                   childData.put("data", "payload " + i + "," +j);
                   childData.put("keyCol", "key" + j);
                   dm.addData("test_child", childData);
                   dm.commit("test_child");
                }
                dm.commit("test_main");
            }
            dm.flush();

            //at this point there should be three rows in test_main with data as "payload i" where i is 0-2
            //each row in test_main should have 3 children with data as "payload i" where i is 0-2

            try {
                con = dm.getConnection();
                Statement stmt = con.createStatement();
                int mainId=-1;
                int count=0;
                ResultSet rs = stmt.executeQuery("SELECT * FROM test_main");
                while(rs.next()){
                    mainId=rs.getInt("id");
                    count++;
                    log.debug(QueryUtils.saveRowAsMap(rs));
                    assertTrue(rs.getString("data").equals("payload "+(mainId)));
                }
                //make sure that an insert happened
                assertTrue(count==3);

                rs = stmt.executeQuery("SELECT * FROM test_child");
                count=0;
                while(rs.next()){
                    count++;
                    log.debug(QueryUtils.saveRowAsMap(rs));
                }
                assertTrue(count==9);

                  //Now that we have verified that there is some data in there lets try to do a lookup
                SqlLookup l = new SqlLookup(dm.getDatabaseSchema());
                l.addSelectColumns("test_main", Arrays.asList("id","data"));
                l.addConstraint("test_main", "keyCol", "key1");
                l.addConstraint("test_child", "keyCol", "notKey");
                
                DmPreparedStatement ps = l.getPreparedStatement(con);
                log.debug(ps.getSql());
                log.debug(ps.getData());
                rs = ps.executeQuery();
                count=0;
                while(rs.next()){
                    log.debug("Match found");
                    log.debug(QueryUtils.saveRowAsMap(rs));
                    count++;
                    assertEquals(rs.getInt("id"),1);
                }
                assertEquals(count,0);

            } catch (SQLException ex) {
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }


        }catch(Exception e){
            log.error("SQL Error", e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }

     /**
     * Test adding a duplicate filter onto the datamanager to perform lookups
      * for matching data based on existing data stored in the datanodes.  If it finds
      * a match it will update the data for that table as opposed to a new row.
      * Note that updateEnabled and/or mergeEnabled must be true.
     */
    @Test
    public void testSqlDuplicateFound() {
        //Initial test setup, create table test_main,test_child
        Connection con = null;
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100), " +
                        "keyCol VARCHAR(10) " +
                        ")";

            stmt.execute(createMain);

            String createChild = "CREATE TABLE test_child (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "main_id INT NOT NULL, " +
                    "data VARCHAR(100), " +
                    "keyCol VARCHAR(10), " +
                    "FOREIGN KEY (main_id) REFERENCES test_main(id) ON DELETE CASCADE ON UPDATE CASCADE" +
                    ")";
            stmt.execute(createChild);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);

        //Test adding data
        try{
            dm.buildSchemas();
            dm.setGlobalMergeEnabled(true);
            dm.setGlobalUpdateEnabled(true);

            //register SqlDuplicateFilter
            SqlLookup l = new SqlLookup(dm.getDatabaseSchema());
            //register the duplicate filter
            //For this duplicate filter a record will be a match if test_main.keyCol
            //matches and test_child.keyCol matches
            SqlDuplicateFilter df = SqlDuplicateFilter.register("test_main", dm);
            df.addConstraint("test_main", "keyCol");
            df.addConstraint("test_child","keyCol");

            //Do initial insert
            for(int i=1;i<=3;i++){
                Map mainData = new HashMap();
                mainData.put("data", "payload " + i);
                mainData.put("keyCol", "key"+i);
                dm.addData("test_main", mainData);
                for(int j=1;j<=3;j++){
                   Map childData = new HashMap();
                   childData.put("data", "payload " + i + "," +j);
                   childData.put("keyCol", "key" + j);
                   dm.addData("test_child", childData);
                   dm.commit("test_child");
                }
                dm.commit("test_main");
            }
            dm.flush();

            //at this point there should be three rows in test_main with data as "payload i" where i is 0-2
            //each row in test_main should have 3 children with data as "payload i" where i is 0-2

            try {
                con = dm.getConnection();
                Statement stmt = con.createStatement();
                int mainId=-1;
                int count=0;
                ResultSet rs = stmt.executeQuery("SELECT * FROM test_main");
                while(rs.next()){
                    mainId=rs.getInt("id");
                    count++;
                    log.debug(QueryUtils.saveRowAsMap(rs));
                    assertTrue(rs.getString("data").equals("payload "+(mainId)));
                }
                //make sure that an insert happened
                assertTrue(count==3);

                rs = stmt.executeQuery("SELECT * FROM test_child");
                count=0;
                while(rs.next()){
                    count++;
                    log.debug(QueryUtils.saveRowAsMap(rs));
                }
                assertTrue(count==9);

                //now we are going to do a search that matches a previously inserted record
                dm.addData("test_main", "keyCol", "key2");
                dm.addData("test_main", "data","UPDATED DATA");
                dm.addData("test_child", "keyCol", "key2");
                //note that the SqlDuplicateFilter that we wrote only applies to
                //test_main, and since we didn't require any unique constraint
                //on test_child.keyCol it will be a new inserted row
                dm.addData("test_child", "data","NEW DATA");
                dm.commit("test_child");
                dm.commit("test_main");
                dm.flush();

                mainId=-1;
                count=0;
                rs = stmt.executeQuery("SELECT * FROM test_main");
                while(rs.next()){
                    log.debug(QueryUtils.saveRowAsMap(rs));
                    count++;
                    mainId=rs.getInt("id");
                    //this is the updated record
                    if(mainId==2){
                        assertEquals(rs.getString("data"),"UPDATED DATA");
                    }else{
                        assertTrue(rs.getString("data").equals("payload "+(mainId)));
                    }
                }
                //make sure that an insert happened
                assertTrue(count==3);

                rs = stmt.executeQuery("SELECT * FROM test_child");
                count=0;
                while(rs.next()){
                    count++;
                    log.debug(QueryUtils.saveRowAsMap(rs));
                }
                assertTrue(count==10);


            } catch (SQLException ex) {
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }


        }catch(Exception e){
            log.error("SQL Error", e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
    
     /**
     * Test performing a lookup based on an IS NULL for a field
     */
    @Test
    public void testLookupNull() {
        //Initial test setup, create table test_main,test_child
        Connection con = null;
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100), " +
                        "keyCol VARCHAR(10) " +
                        ")";

            stmt.execute(createMain);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);

        //Test adding data
        try{
            dm.buildSchemas();
            dm.setGlobalMergeEnabled(true);
            dm.setGlobalUpdateEnabled(true);

            //register SqlDuplicateFilter

            //register the duplicate filter
            //For this duplicate filter a record will be a match if test_main.keyCol
            //matches and test_child.keyCol matches
            SqlDuplicateFilter df = SqlDuplicateFilter.register("test_main", dm);
            df.addConstraint("test_main", "data");            

            //Do initial insert
            for(int i=1;i<3;i++){
                Map mainData = new HashMap();
                mainData.put("data", "payload " + i);
                mainData.put("keyCol", "key"+i);
                dm.addData("test_main", mainData);
                dm.commit("test_main");                
            }
            dm.flush();
            
            Map nullData = new HashMap();
            nullData.put("data", null);
            nullData.put("keyCol", "keyNull");               
            dm.addData("test_main", nullData);
            dm.commit("test_main");  
            dm.flush();
            Map nullData2 = new HashMap();
            nullData2.put("data", null);
            nullData2.put("keyCol", "updatedKey");
            dm.addData("test_main", nullData2);
            dm.commit("test_main");                
            dm.flush();          
        }catch(Exception e){
            log.error("SQL Error", e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }

}