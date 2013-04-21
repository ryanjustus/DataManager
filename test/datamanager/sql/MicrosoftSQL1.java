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
public class MicrosoftSQL1 {

    
    /**
     * Modify settings for your database installation
     */
    static final String dbUser = "tauber",
            dbPass="tauber",
            database="tauber",
            dbUrl = "jdbc:sqlserver://192.168.1.103:1433;",
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static BasicDataSource ds;
    private static final Logger log = Logger.getLogger(MicrosoftSQL1.class);
    public MicrosoftSQL1() {

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
        
        
        log.info("Starting test suite for Microsoft Sql Server");
        // This will load the MySQL driver, each DB has its own driver
        Class.forName(driver);
        // Setup the connection with the DB
        Connection con=null;

        try{
            
            ds = new BasicDataSource();
            ds.setDriverClassName( driver );
            ds.setUsername(dbUser );
            ds.setPassword(dbPass);
            ds.setUrl(dbUrl+"databaseName="+database+";");
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

  
    
    @Test 
    public void testPriceInsert(){
        
        log.info("Testing addData");
        Connection con = null;     
        
         SqlDataManager dm = new SqlDataManager(ds);
        //Test adding data
        try{
            
            dm.buildSchemas();            
            Set<RelationalSchema> ss = dm.getDatabaseSchema().getRelationalSchemas();
            Iterator<RelationalSchema> itr = ss.iterator();
            while(itr.hasNext()){
                RelationalSchema s = itr.next();
                System.out.println(s.toString());        
            }
            
            dm.addData("price","price_name","Price");
            dm.addData("price","amount","179.99");
            dm.commit("price");
            dm.flush();
            
 
            try{
                con = dm.getConnection();
                PreparedStatement ps = con.prepareStatement("SELECT * FROM price");
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                String amount = rs.getString("amount");
                assertEquals(amount,"179.99");
            }catch(SQLException e){
            }finally{
                con.close();
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            dm.close();
        }        
    }    
    
}