/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datamanager.sql;

import com.screenscraper.datamanager.sql.SqlDataManager;
import com.screenscraper.datamanager.sql.SqlSchemaBuilder;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import com.screenscraper.datamanager.DatabaseSchema;
import com.screenscraper.datamanager.RelationalSchema;
import java.util.Iterator;
import java.util.Set;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.Connection;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.commons.dbcp.BasicDataSource;
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
public class ForeignKeyTest {
    
     
     /**
     * Modify settings for your database installation
     */
    static final String dbUser = "root",
            dbPass="u$3tw1k1",
            //dbPass="",
            database="DataManagerTest",
            dbUrl = "jdbc:mysql://ls.ekiwi.net:3306/",
            //dbUrl = "jdbc:mysql://localhost:3306/",
            driver = "com.mysql.jdbc.Driver";
    static final int numChild=10;
    static BasicDataSource ds;
    private static final Logger log = Logger.getLogger(MySql.class);
    public ForeignKeyTest() {

    }

    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException, SQLException{
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
    
        @Test
    public void testParseForeignKey1() {
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
            for(int i=0;i<numChild;i++){
                String createChild = "CREATE TABLE test_child_"+i+" (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "main_id INT NOT NULL, " +
                        "data VARCHAR(100), " +
                        "INDEX(main_id), " +
                        "FOREIGN KEY (main_id) REFERENCES test_main(id) ON DELETE CASCADE ON UPDATE CASCADE" +
                        ")";
                stmt.execute(createChild);
            }
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }

        SqlDataManager dm = new SqlDataManager(ds);        
        dm.buildSchemas();        
        SqlSchemaBuilder sb = new SqlSchemaBuilder(dm);
        Set<RelationalSchema> ss = dm.getDatabaseSchema().getRelationalSchemas();
        
        con = null;
        long start = System.currentTimeMillis();
        try{
            con=ds.getConnection();
            for(RelationalSchema s: dm.getDatabaseSchema().getRelationalSchemas()){                
                sb.parseForeignKeys(s,dm.getDatabaseSchema(), con.getMetaData());
            }
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        long elapsed = System.currentTimeMillis()-start;
        System.out.println("TOTAL TIME TO PARSE FK: "+elapsed);
        for(RelationalSchema s: dm.getDatabaseSchema().getRelationalSchemas()){
            System.out.println(s);
        } 
        dm.close();
    }
        
    @Test
    public void testParseForeignKey2() {
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
            for(int i=0;i<numChild;i++){
                String createChild = "CREATE TABLE test_child_"+i+" (" +
                        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "main_id INT NOT NULL, " +
                        "data VARCHAR(100), " +
                        "INDEX(main_id), " +
                        "FOREIGN KEY (main_id) REFERENCES test_main(id) ON DELETE CASCADE ON UPDATE CASCADE" +
                        ")";
                stmt.execute(createChild);
            }
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }

        SqlDataManager dm = new SqlDataManager(ds);        
        dm.buildSchemas();        
        SqlSchemaBuilder sb = new SqlSchemaBuilder(dm);
        con = null;
        long start = System.currentTimeMillis();
        try{
            con=ds.getConnection();
            for(RelationalSchema s: dm.getDatabaseSchema().getRelationalSchemas()){
                sb.parseForeignKeys2(s, dm.getDatabaseSchema(), con.getMetaData());
            }
        }catch(SQLException e){
            e.printStackTrace(System.err);
            assertTrue(false);
        }finally{
            try{con.close();}catch(SQLException e){}
        }
        long elapsed = System.currentTimeMillis()-start;
        System.out.println("TOTAL TIME TO PARSE FK: "+elapsed);
        for(RelationalSchema s: dm.getDatabaseSchema().getRelationalSchemas()){
            System.out.println(s);
        }        
        dm.close();
    }
}
