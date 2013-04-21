/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.sql;

import com.screenscraper.datamanager.sql.SqlDataManager;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import java.util.Iterator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.screenscraper.datamanager.DataObject;
import java.util.Map;
import java.util.HashMap;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.sql.SqlDatabaseSchema;
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
 * This test requires Microsoft SQL Server installed.  Connection information
 * is set below.
 * Tested using Microsoft SQL Server 2008 R2 Express 64 bit with sqljdbc4 driver
 * NOTE:  It will create and use a database named DataManagerTest.
 * @author ryan
 */
public class MsAccess {

    
    /**
     * Modify settings for your database installation
     */
    static final String dbUser = "",
            dbPass="",
            database="datamanagertest",
            dbUrl = "jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:/Users/ryan/Documents/datamanagertest.accdb",
            driver = "sun.jdbc.odbc.JdbcOdbcDriver";
    static BasicDataSource ds;
    private static final Logger log = Logger.getLogger(MicrosoftSQL.class);
    public MsAccess() {

    }

    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException{
       BasicConfigurator.configure();
    }

    @AfterClass
    public static void tearDownClass(){
    }

    @Before
    public void setUp() throws ClassNotFoundException {
        
        
        log.info("Starting test suite for Microsoft Access");
        // This will load the MySQL driver, each DB has its own driver
        Class.forName(driver);
        // Setup the connection with the DB
        Connection con=null;
        try{
            con = DriverManager.getConnection(dbUrl,"","");
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM Table1");
            while(rs.next()){
                log.info(rs.getString("ID"));
            }
            
        }catch(SQLException e){
            log.error("ERROR: unable to establish connection", e);
        }finally{
            if(con!=null)
                try{con.close();}catch(SQLException e){log.error("failed to close connection", e);
                }
        }
        con = null;
        try{
            
            ds = new BasicDataSource();
            ds.setDriverClassName( driver );
            ds.setUsername(dbUser );
            ds.setPassword(dbPass);
            ds.setUrl(dbUrl);
            con=ds.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as val"); 
            rs.next();
            log.debug("DATABASE CONNECTION TEST: "+rs.getInt("val"));
          //  stmt.execute("SET FOREIGN_KEY_CHECKS=1");
        }catch(SQLException e){
            log.error("Error connecting",e);
            assertTrue(false);
        }finally{
            try{con.close();}catch(Exception e){
            }
        }
    }

    @After
    public void tearDown() throws SQLException {
        if(ds!=null){
             log.debug("Closing datasource");
             ds.close();
             
        }
    }
    
       /**
    * Test parsing out of the schemas.
    * Create two tables with a one-to-many relationship, test_main and test_child
    */    
    @Test
    public void testGetMetaData() throws SQLException {
      
        Connection con = ds.getConnection();
        DatabaseMetaData md= con.getMetaData();
        ResultSet rs = md.getColumns(null, null, "Table1", null);
        while(rs.next()){
            log.info(rs.getString("COLUMN_NAME"));
            
        }
    } 
    
    
    /**
    * Test parsing out of the schemas.
    * Create two tables with a one-to-many relationship, test_main and test_child
    */    
    @Test
    public void testBuildSchemas() {
      
        SqlDataManager dm = new SqlDataManager(ds);
        dm.buildSchemas();

        dm.close();
    }   

  
}