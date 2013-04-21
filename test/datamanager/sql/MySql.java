/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.sql;

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
public class MySql {

   
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
    public MySql() {

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
                    "data VARCHAR(100), " +
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
        dm.buildSchemas();        
        Set<RelationalSchema> ss = dm.getDatabaseSchema().getRelationalSchemas();
        assertEquals(ss.size(),2);
        Iterator<RelationalSchema> itr = ss.iterator();
        while(itr.hasNext()){
            RelationalSchema s = itr.next();
            System.out.println(s.toString());
            itr.remove();            
        }
        assertTrue(ss.isEmpty());
        dm.close();
    }   

    /**
     * Test of getPrimaryKeys method, of class SqlDataManager.
     * Creates one table, test_main with a primary key composed of two
     * columns and tests to see if it was parsed
     */
    
    @Test
    public void testGetPrimaryKeys() {

        Connection con = null;
        try{

            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL, " +
                        "id2 VARCHAR(5), " +
                        "data VARCHAR(100), " +
                        "cur_timestamp TIMESTAMP, " +
                        "status TINYINT, " +
                        "PRIMARY KEY(id,id2)" +
                    ")";

            stmt.execute(createMain);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        try{
            dm.buildSchemas();
            System.out.println("getPrimaryKeys");
            Set result = dm.getPrimaryKeys("test_main");
            Set expResult = new HashSet(Arrays.asList("id","id2"));
            assertEquals(expResult, result);
        } catch(Exception e){
            assertTrue(false);
        }finally{
           dm.close();
        }
    }

     /**
     * Test adding data using various cases with a one-to-many relationship
     */ 
    @Test
    public void testAddData() {
        //Initial test setup, create table test_main,test_child
        Connection con = null;
        try{
            con=ds.getConnection();
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
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            dm.buildSchemas();
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
            dm.commit("test_mAin");
            dm.flush();

            
             //at this point we should have the following data in the database:
             // main {(id,AUTO_INCREMENT),(data,"payload")}
             // child {(id,AUTO_INCREMENT),(main_id,main.id),(data,"child payload")}

            try {
                con = dm.getConnection();
                Statement stmt = con.createStatement();
                int mainId=-1;
                ResultSet rs = stmt.executeQuery("SELECT id,data FROM test_main");
                while(rs.next()){
                    mainId=rs.getInt("id");
                    assertTrue(rs.getString("data").equals("payload"));
                }
                //make sure that an insert happened
                assertTrue(mainId>=0);

                rs = stmt.executeQuery("SELECT id,main_id,data FROM test_child");
                int childId = -1;
                while(rs.next()){
                    childId = rs.getInt("id");
                    assertEquals(mainId,rs.getInt("main_id"));
                    assertTrue(rs.getString("data").equals("child payload"));
                }
                assertTrue(childId>=0);

            } catch (SQLException ex) {
                log.error("Lookup error ", ex);
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            e.printStackTrace(System.err);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
    
       /**
     * Test adding data using various cases with a one-to-many relationship
     */ 
    @Test
    public void testAddTinyInt1() {
        //Initial test setup, create table test_main,test_child
        Connection con = null;
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data TINYINT(1) "+
                        ")";
            stmt.execute(createMain);

        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            dm.buildSchemas();
            System.out.println("add data");
            //Do initial insert using case insentivity for tables and columns
            dm.addData("test_main","data",1);
            dm.commit("test_main");
            dm.flush();

            try {
                con = dm.getConnection();
                Statement stmt = con.createStatement();
                int mainId=-1;
                ResultSet rs = stmt.executeQuery("SELECT id,data FROM test_main");
                while(rs.next()){
                    mainId=rs.getInt("id");
                    assertTrue(rs.getInt("data")==1);
                    assertTrue(rs.getBoolean("data"));
                }
                //make sure that an insert happened
                assertTrue(mainId>=0);

            } catch (SQLException ex) {
                log.error("Lookup error ", ex);
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            e.printStackTrace(System.err);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
    
    

    @Test
    /**
     * Test merge data. Merge only updates null values on the database
     */
    public void testMerge() {

        Connection con = null;
        try{

            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain =
                    "CREATE TABLE test_main (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "data VARCHAR(100), " +
                    "data2 VARCHAR(100), " +
                    "PRIMARY KEY(id)" +
                    ")";
            stmt.execute(createMain);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        try{
            dm.buildSchemas();
            dm.setMergeEnabled("test_main", true);

            //add initial data
            dm.addData("test_main", "data", "payload");
            dm.commit("test_main");
            dm.flush();

            //add updated data
            DataObject pk = dm.getLastAutoIncrementValue("test_main");
            dm.addData("test_main", "id",pk);
            dm.addData("test_main","data","update payload");
            dm.addData("test_main","data2","merge payload");
            dm.commit("test_main");
            dm.flush();

            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_main");
                ResultSet rs = ps.executeQuery();
                int count = 0;
                while(rs.next()){
                    count++;
                    //verify that there is still only one row
                    assertEquals(count,1);
                    //verify that the existing data wasn't updated
                    assertEquals(rs.getString("data"), "payload");
                    //verify that the existing data wasn't updated
                    assertEquals(rs.getString("data2"), "merge payload");
                }
                //make sure that an insert occured
                assertTrue(count>0);
            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }            
        }catch(Exception e){
            assertTrue(false);
        } finally {
           dm.close();
        }
    }


    /**
     * Test update data. Update only updates non-null values on the database.
     */
    @Test
    public void testUpdate() {

        Connection con = null;
        try{

            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain =
                    "CREATE TABLE test_main (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "data VARCHAR(100), " +
                    "data2 VARCHAR(100), " +
                    "PRIMARY KEY(id)" +
                    ")";
            stmt.execute(createMain);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        try{
            dm.buildSchemas();
            dm.setUpdateEnabled("test_main", true);

            //add initial data
            dm.addData("test_main", "data", "payload");
            dm.commit("test_main");
            dm.flush();

            //add updated data
            DataObject pk = dm.getLastAutoIncrementValue("test_main");
            dm.addData("test_main", "id",pk);
            dm.addData("test_main","data","update payload");
            dm.addData("test_main","data2","merge payload");
            dm.commit("test_main");
            dm.flush();

            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_main");
                ResultSet rs = ps.executeQuery();
                int count = 0;
                while(rs.next()){
                    count++;
                    //verify that there is still only one row
                    assertEquals(count,1);
                    //verify that the existing data was updated
                    assertEquals(rs.getString("data"), "update payload");
                    //verify that data2 is still null
                    assertEquals(rs.getString("data2"), null);
                }
                //make sure that an insert occured
                assertTrue(count>0);
            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            assertTrue(false);
        }finally{
           dm.close();
        }
    }

     /**
     * Test the auto creation of a many-to-many relationship
     */
    @Test
    public void testAutoManyToMany() {

        Connection con = null;
        try{

            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain =
                    "CREATE TABLE main (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "data VARCHAR(100), " +
                    "PRIMARY KEY(id)" +
                    ")";
            stmt.execute(createMain);

            String createAttr =
                    "CREATE TABLE attr (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "name VARCHAR(20), " +
                    "value VARCHAR(20), " +
                    "PRIMARY KEY(id) " +
                    ")";
            stmt.execute(createAttr);

            String createMainHasAttr =
                    "CREATE TABLE main_has_attr (" +
                    "main_id INT, " +
                    "attr_id INT, " +
                    "PRIMARY KEY(main_id, attr_id), " +
                    "FOREIGN KEY (main_id) REFERENCES main(id) ON DELETE CASCADE ON UPDATE CASCADE, " +
                    "FOREIGN KEY (attr_id) REFERENCES attr(id) ON DELETE CASCADE ON UPDATE CASCADE " +
                    ")";
            stmt.execute(createMainHasAttr);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //enable auto many-to-many
        dm.setAutoManyToMany(true);
        try{
            dm.buildSchemas();
            //add initial data
            dm.addData("main", "data", "payload");


            dm.addData("attr", "name", "attr1");
            dm.addData("attr", "value", "val1");
            dm.commit("main");
            dm.commit("attr");
            dm.flush();
            DataObject attrPk = dm.getLastAutoIncrementValue("attr");
            DataObject mainPk = dm.getLastAutoIncrementValue("main");

            //add updated data
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM main_has_attr");
                ResultSet rs = ps.executeQuery();
                int count = 0;
                while(rs.next()){
                    count++;
                    //verify that there is still only one row
                    assertEquals(count,1);
                    //verify that main_pk is correct
                    assertEquals(rs.getInt("main_id"), mainPk.getObject());
                    //verify that attr_pk is correct
                    assertEquals(rs.getInt("attr_id"), attrPk.getObject());
                }
                //make sure that an insert occured
                assertTrue(count>0);
            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            e.printStackTrace(System.err);
            assertTrue(false);
        } finally {
           dm.close();
        }
    }

     /**
     * Test the auto creation of a many-to-many relationship
     * In this case a many-to-many relationship should not be made because
     * a constraint fails on the insert
     */
    @Test
    public void testAutoManyToManyNegative() {

        Connection con = null;
        try{

            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain =
                    "CREATE TABLE main (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "data VARCHAR(100), " +
                    "PRIMARY KEY(id)" +
                    ")";
            stmt.execute(createMain);

            String createAttr =
                    "CREATE TABLE attr (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "name VARCHAR(20), " +
                    "value VARCHAR(20), " +
                    "PRIMARY KEY(id) " +
                    ")";
            stmt.execute(createAttr);

            String createMainHasAttr =
                    "CREATE TABLE main_has_attr (" +
                    "main_id INT, " +
                    "attr_id INT, " +
                    "notNull VARCHAR(10) NOT NULL, " +
                    "PRIMARY KEY(main_id, attr_id), " +
                    "FOREIGN KEY (main_id) REFERENCES main(id) ON DELETE CASCADE ON UPDATE CASCADE, " +
                    "FOREIGN KEY (attr_id) REFERENCES attr(id) ON DELETE CASCADE ON UPDATE CASCADE " +
                    ")";
            stmt.execute(createMainHasAttr);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //enable auto many-to-many
        dm.setAutoManyToMany(true);
        try{
            dm.buildSchemas();
            //add initial data
            dm.addData("main", "data", "payload");


            dm.addData("attr", "name", "attr1");
            dm.addData("attr", "value", "val1");
            dm.commit("main");
            dm.commit("attr");
            dm.flush();
            DataObject attrPk = dm.getLastAutoIncrementValue("attr");
            DataObject mainPk = dm.getLastAutoIncrementValue("main");

            //add updated data
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM main_has_attr");
                ResultSet rs = ps.executeQuery();
                int count = 0;
                while(rs.next()){
                    count++;
                }
                //make sure that an insert occured
                assertTrue(count==0);
            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            e.printStackTrace(System.err);
            assertTrue(false);
        } finally {
           dm.close();
        }
    }

     /**
     * Test the auto creation of a many-to-many relationship
     * In this case a many-to-many relationship should succeed because
     * even though a column with a not null isn't set, there is a default
     * value for that column
     */
    @Test
    public void testAutoManyToManyDefault() {

        Connection con = null;
        try{

            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain =
                    "CREATE TABLE main (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "data VARCHAR(100), " +
                    "PRIMARY KEY(id)" +
                    ")";
            stmt.execute(createMain);

            String createAttr =
                    "CREATE TABLE attr (" +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "name VARCHAR(20), " +
                    "value VARCHAR(20), " +
                    "PRIMARY KEY(id) " +
                    ")";
            stmt.execute(createAttr);

            String createMainHasAttr =
                    "CREATE TABLE main_has_attr (" +
                    "main_id INT, " +
                    "attr_id INT, " +
                    "notNull VARCHAR(10) NOT NULL DEFAULT 'DEFAULT', " +
                    "PRIMARY KEY(main_id, attr_id), " +
                    "FOREIGN KEY (main_id) REFERENCES main(id) ON DELETE CASCADE ON UPDATE CASCADE, " +
                    "FOREIGN KEY (attr_id) REFERENCES attr(id) ON DELETE CASCADE ON UPDATE CASCADE " +
                    ")";
            stmt.execute(createMainHasAttr);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //enable auto many-to-many
        dm.setAutoManyToMany(true);
        try{
            dm.buildSchemas();
            //add initial data
            dm.addData("main", "data", "payload");


            dm.addData("attr", "name", "attr1");
            dm.addData("attr", "value", "val1");
            dm.commit("main");
            dm.commit("attr");
            dm.flush();
            DataObject attrPk = dm.getLastAutoIncrementValue("attr");
            DataObject mainPk = dm.getLastAutoIncrementValue("main");

            //add updated data
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM main_has_attr");
                ResultSet rs = ps.executeQuery();
                int count = 0;
                while(rs.next()){
                    count++;
                    assertEquals(count,1);
                    assertEquals(rs.getString("notNull"),"DEFAULT");
                }
                //make sure that an insert occured
                assertTrue(count>0);
            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            e.printStackTrace(System.err);
            assertTrue(false);
        } finally {
           dm.close();
        }
    }
    
    /**
    * Test updating data using various cases
    */
    
    @Test
    public void testParentChildUpdateMergeData(){

        Connection con = null;
        //Set up by creating two tables: test_main and test_child with test_child.main_id -> main.id
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100), " +
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
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            log.error(e.getMessage(), e);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            dm.buildSchemas();
            dm.setUpdateEnabled("tEst_child", true);
            dm.setMergeEnabled("test_chIld", true);
            dm.setUpdateEnabled("tEst_Main", true);
            dm.setMergeEnabled("test_main", true);

            //add some initial data to the database
            dm.addData("test_main", "data",null);
            dm.addData("test_main", "id", "100");
            dm.addData("test_child", "data","orig child payload");
            dm.commit("test_child");
            dm.commit("test_main");
            dm.flush();

            //add in pk so that an update can be performed
            DataObject mainPk = dm.getLastAutoIncrementValue("test_main");
            dm.addData("test_main", "id", mainPk);
            //merge test_main.data
            dm.addData("test_main", "data", "payload");
            

            //add in pk so that an update will performed
            DataObject childPk = dm.getLastAutoIncrementValue("test_child");
            dm.addData("test_child", "id", childPk);
            //update test_child.data
            dm.addData("tESt_child", "data","new child payload");
            dm.commit("test_child");
            dm.commit("test_main");
            dm.flush();

            
            // at this point we should have the following data in the database:
            // main {(id,AUTO_INCREMENT,),(data,payload)}
            // child {(id,AUTO_INCREMENT),(main_id,main.id),(data,"new child payload")}
             
            try {
                con = dm.getConnection();
                Statement stmt = con.createStatement();
                int mainId=-1;
                ResultSet rs = stmt.executeQuery("SELECT id,data FROM test_main");
                int numRows=0;
                while(rs.next()){
                    numRows++;
                    assertEquals(numRows,1);
                    mainId=rs.getInt("id");
                    assertEquals(mainId, mainPk.getObject());
                    assertEquals(rs.getString("data"),"payload");
                }
                //make sure an data is actually there
                assertTrue(numRows>0);

                rs = stmt.executeQuery("SELECT id,main_id,data FROM test_child");
                numRows=0;
                while(rs.next()){
                    numRows++;
                    assertEquals(numRows,1);
                    assertEquals(mainId,rs.getInt("main_id"));
                    assertTrue(rs.getString("data").equals("new child payload"));
                }
                //make sure an data is actually there
                assertTrue(numRows>0);
            } catch (SQLException ex) {
                log.error(ex);
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }


        }catch(Exception e){
            log.error(e.getMessage(), e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
    
    
    
    
    /**
    * Test updating data using various cases
    */
    
    @Test
    public void testParentChildUpdateMergeDataNoChange(){

        Connection con = null;
        //Set up by creating two tables: test_main and test_child with test_child.main_id -> main.id
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "uk INT NOT NULL UNIQUE KEY, " +
                        "data VARCHAR(100), " +
                        "status TINYINT," +
                        "UNIQUE INDEX uk_idx (uk)"+                       
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
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            log.error(e.getMessage(), e);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            dm.buildSchemas();
            dm.setUpdateEnabled("tEst_child", true);
            dm.setMergeEnabled("test_chIld", true);
            dm.setUpdateEnabled("tEst_Main", true);
            dm.setMergeEnabled("test_main", true);

            //add some initial data to the database
            dm.addData("test_main", "data","payload");
            dm.addData("test_child", "data","orig child payload");
            dm.addData("test_main","uk",1);
            dm.commit("test_child");
            dm.commit("test_main");
            dm.flush();
            
            dm.logDebug("******************************************************");
            
            dm.addData("test_main","uk",1);
            dm.addData("test_main", "data", "payload");
            
          
            //update test_child.data
            dm.addData("tESt_child", "data","second child payload");
            dm.commit("test_child");
            dm.commit("test_main");
            dm.flush();

            
            // at this point we should have the following data in the database:
            // main {(id,AUTO_INCREMENT,),(data,payload)}
            // child {(id,AUTO_INCREMENT),(main_id,main.id),(data,"new child payload")}
             
            try {
                con = dm.getConnection();
                Statement stmt = con.createStatement();
                int mainId=-1;
                ResultSet rs = stmt.executeQuery("SELECT id,data,uk FROM test_main");
                int numRows=0;
                while(rs.next()){
                    numRows++;
                    assertEquals(numRows,1);
                    int uk=rs.getInt("uk");
                    assertEquals(uk, 1);
                    assertEquals(rs.getString("data"),"payload");
                }
                //make sure an data is actually there
                assertTrue(numRows>0);

                rs = stmt.executeQuery("SELECT id,main_id,data FROM test_child");
                numRows=0;
                while(rs.next()){
                    numRows++;
                    assertEquals(1,rs.getInt("main_id"));
                    assertTrue(rs.getString("data").contains("child payload"));
                }
                //make sure an data is actually there
                assertTrue(numRows>0);
            } catch (SQLException ex) {
                ex.printStackTrace(System.err);
                log.error(ex);
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }


        }catch(Exception e){
            log.error(e.getMessage(), e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }


    /**
     * Test updating data in a table with multiple key constraints
     * 1 primary key, 1 unique index
     */
    
    @Test
    public void testAddDataUniqueKeyConstraint() {

        Connection con = null;
        try{

            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createTableMultipleKey = "CREATE TABLE test_multiple_key (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "data VARCHAR(100), " +
                    "key1 VARCHAR(10) NOT NULL, " +
                    "key2 VARCHAR(10) NOT NULL, " +
                    "CONSTRAINT UNIQUE INDEX uKey (key1,key2) " +
                    ")";
            stmt.execute(createTableMultipleKey);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        SqlDataManager dm = new SqlDataManager(ds);
        try{
            dm.buildSchemas();

            dm.setUpdateEnabled("test_multiple_key", true);
            dm.setMergeEnabled("test_multiple_key", true);

            //Do initial insert
            Map data = new HashMap();
            data.put("data", "tst data");
            data.put("key1", "1");
            data.put("key2", 2);
            dm.addData("test_multiple_key", data);
            dm.commit("test_multiple_key");
            dm.flush();
            
            //update via pk
            data.clear();
            data.put("id",dm.getLastAutoIncrementValue("test_multiple_key"));
            data.put("data", "tst data updated via pk");
            dm.addData("test_multiple_key", data);
            dm.commit("test_multiple_key");
            dm.flush();
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_multiple_key");
                ResultSet rs = ps.executeQuery();
                int count = 0;
                while(rs.next()){
                    count++;
                    //verify that there is still only one row
                    assertEquals(count,1);
                    //verify that the database column was updated
                    assertEquals(rs.getString("data"), data.get("data"));
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }

            //update via unique key combo, note I switch which was input as a
            //a string from the initial insert, this should still work
            data.clear();
            data.put("key1",1);
            data.put("key2", "2");
            data.put("data", "tst data updated via unique index");
            dm.addData("test_multiple_key", data);
            dm.commit("test_multiple_key");
            dm.flush();
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_multiple_key");
                ResultSet rs = ps.executeQuery();
                int count = 0;
                while(rs.next()){
                    count++;
                    assertEquals(count,1);
                    assertEquals(rs.getString("data"), data.get("data"));
                }
            } catch (SQLException ex) {
                ex.printStackTrace(System.err);
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            assertTrue(false);
        }finally{
            dm.close();
        }
    }

    



    /**
     * Test adding to 'time' fields in the database with various inputs
     */
    
    @Test
    public void testDates() throws ParseException {

        Connection con = null;
        //Set up by creating a table test_date with various date fields
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createDate = "CREATE TABLE test_date (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "c_date DATE, " +
                    "c_time TIME, " +
                    "c_datetime DATETIME, " +
                    "c_timestamp TIMESTAMP" +
                  ")";
            stmt.execute(createDate);

        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);
        try{
            dm.buildSchemas();
            //Test adding data

            RelationalSchema s = dm.getSchema("test_date");
            //add formatters
            SimpleDateFormat fDate = new SimpleDateFormat("MM/dd/yy");
            s.setDateFormat("c_date", fDate);
            SimpleDateFormat fTime = new SimpleDateFormat("HH:mm");
            s.setDateFormat("c_time", fTime);
            SimpleDateFormat fDateTime = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
            s.setDateFormat("c_datetime", fDateTime);
            SimpleDateFormat fTimestamp = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
            s.setDateFormat("c_timestamp", fTimestamp);

            //add various data that will be parsed from a string;
            String date =  "12/05/81";
            dm.addData("test_date", "c_date", date);
            String time = "12:00:15";
            dm.addData("test_date", "c_time", time);
            String datetime = "12/05/1981 12:00:15";
            dm.addData("test_date", "c_datetime", datetime);
            String timestamp = "12/05/1981 12:00:15";
            dm.addData("test_date", "c_timestamp", timestamp);
            dm.commit("test_date");
            dm.flush();
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_date");
                ResultSet rs = ps.executeQuery();
                rs.next();
                assertEquals(rs.getDate("c_date"), new java.sql.Date(fDate.parse(date).getTime()));
                assertEquals(rs.getTime("c_time"), new java.sql.Time(fTime.parse(time).getTime()));
                assertEquals(rs.getTimestamp("c_datetime"), new java.sql.Timestamp(fDateTime.parse(datetime).getTime()));
                assertEquals(rs.getTimestamp("c_timestamp"), new java.sql.Timestamp(fTimestamp.parse(timestamp).getTime()));
                ps.executeQuery("TRUNCATE TABLE test_date");

            } catch (SQLException ex) {
                ex.printStackTrace(System.err);
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }

            //add date using Sql types, no parsing
            java.sql.Time sqlTime = new java.sql.Time(System.currentTimeMillis());
            dm.addData("test_date", "c_Time", sqlTime);
            dm.commit("test_date");
            dm.flush();
            con = null;
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_date");
                ResultSet rs = ps.executeQuery();
                rs.next();
                assertEquals(rs.getTime("c_time").toString(), sqlTime.toString());
                ps.executeQuery("TRUNCATE TABLE test_date");

            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }

             //add date using numeric unix time, parse from number
            long now = System.currentTimeMillis();
            dm.addData("test_date", "c_timestamp", now);
            dm.commit("test_date");
            dm.flush();
            con = null;
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_date");
                ResultSet rs = ps.executeQuery();
                rs.next();
                //unfortunately mysql timestamp only has a resolution of 1 second so we
                //have to round for this test
                assertTrue(now-rs.getTimestamp("c_timestamp").getTime()<1000);
               // ps.executeQuery("TRUNCATE TABLE test_date");

            } catch (SQLException ex) {
                ex.printStackTrace();
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
        }catch(Exception e){
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
    
     /**
     * Test adding to 'time' fields in the database with various inputs
     */
    
    @Test
    public void testDates2() throws ParseException {

        Connection con = null;
        //Set up by creating a table test_date with various date fields
        try{
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            String createDate = "CREATE TABLE test_date (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "c_date DATE "+
                  ")";
            stmt.execute(createDate);

        }catch (SQLException e) {
            e.printStackTrace(System.err);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);
        try{
            dm.buildSchemas();
            //Test adding data

            RelationalSchema s = dm.getSchema("test_date");
            //add formatters
            SimpleDateFormat fDate = new SimpleDateFormat("MMMMM d, yyyy");
            dm.setDateFormat("test_date", "c_date", fDate);

            //add various data that will be parsed from a string;
            String date =  "January 23, 2011";
            Map data = new HashMap();
            data.put("C_DATE",date);
            dm.addData("test_date", data);           
            dm.commit("test_date");
            dm.flush();
            try {
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM test_date");
                ResultSet rs = ps.executeQuery();
                rs.next();
                assertEquals(rs.getDate("c_date"), new java.sql.Date(fDate.parse(date).getTime()));                
                ps.executeQuery("TRUNCATE TABLE test_date");

            } catch (SQLException ex) {
                ex.printStackTrace(System.err);
                assertTrue(false);
            } finally{
                try{con.close();}catch(Exception e){}
            }
          
        }catch(Exception e){
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
    
    
    /**
     * @TODO add asserts.  Right now it is tested by inspection of log file
    * Test updating data using various cases
    */
    
    @Test
    public void testUpdateChild(){

        Connection con = null;
        //Set up by creating two tables: test_main and test_child with test_child.main_id -> main.id
        try{
            con=ds.getConnection();
            
            Statement stmt = con.createStatement();
            String createMain = "CREATE TABLE test_main (" +
                        "pk INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "data VARCHAR(100), " +
                        "cur_timestamp TIMESTAMP, " +
                        "status TINYINT" +
                        ")";
            stmt.execute(createMain);
            
            stmt = con.createStatement();
            String createCache = "CREATE TABLE test_cache (" +
                        "hash VARCHAR(32) NOT NULL PRIMARY KEY, " +
                        "data VARCHAR(100), " +
                        "cur_timestamp TIMESTAMP, " +
                        "status TINYINT" +
                        ")";
            stmt.execute(createCache);

            String createMainHasCache = "CREATE TABLE test_main_has_cache (" +
                    "main_pk INT NOT NULL, " +
                    "cache_pk VARCHAR(32) NOT NULL, " +
                    "page_source VARCHAR(100), " +
                    "INDEX(main_pk), " +
                    "PRIMARY KEY(main_pk,cache_pk), "+
                    "FOREIGN KEY (main_pk) REFERENCES test_main(pk) ON DELETE CASCADE ON UPDATE CASCADE, " +
                    "FOREIGN KEY (cache_pk) REFERENCES test_cache(hash) ON DELETE CASCADE ON UPDATE CASCADE" +
                    ")";
            stmt.execute(createMainHasCache);
        }catch (SQLException e) {
            e.printStackTrace(System.err);
            log.error(e.getMessage(), e);
            assertTrue(false);
        }
        SqlDataManager dm = new SqlDataManager(ds);
        dm.buildSchemas();
        //initialize an existing entry in test_cache
        String hash = "123456";
        dm.addData("test_cache","hash",hash);
        dm.addData("test_cache","data","This is my cached data");
        dm.commit("test_cache");
        dm.flush();
        
        //Now create some data and try to link it to that existing data
        dm.addData("test_main","data", "main data");
        
        dm.addData("test_cache","hash",hash);
        dm.addData("test_cache","data","This is my cached data");
        
        dm.addData("test_main_has_cache","cache_pk",hash);
        dm.addData("test_main_has_cache","page_source","test insert");
        
        dm.commit("test_main_has_cache");
        dm.commit("test_cache");
        dm.commit("test_main"); 
        dm.flush();
        
        
        //Test adding data
        try{
            dm.buildSchemas();

        }catch(Exception e){
            log.error(e.getMessage(), e);
            assertTrue(false);
        } finally {
            dm.close();
        }
    }
}