/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datamanager.sql;

import com.screenscraper.datamanager.sql.SshDataSource;
import org.apache.log4j.Logger;
import java.text.ParseException;
import com.screenscraper.datamanager.sql.util.QueryUtils;
import java.sql.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
public class SshMysqlTest {

   
     /**
     * Modify settings for your database installation
     */
    static final String dbUser = "root",
            dbPass="password",
            database="DataManagerTest",
            dbUrl = "jdbc:mysql://127.0.0.1:3306/",
            driver = "com.mysql.jdbc.Driver",
            remoteUrl="example.com";
    private static final Logger log = Logger.getLogger(SshMysqlTest.class);
    public SshMysqlTest() {

    }

    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException{
       
    }

    @AfterClass
    public static void tearDownClass(){
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
    }

      @Test
    public void testConnect() throws ParseException, SQLException {
         SshDataSource ds = new SshDataSource("user@remotehost.com", "sshPassword");
          // SshDataSource
         ds.setDriverClassName( "com.mysql.jdbc.Driver" );
         ds.setUsername( "root" );
         ds.setPassword( dbPass );

         // Accepted values for the first parameter of setUrl are:
         //   SshDataSource.Type.MYSQL
         //   SshDataSource.Type.MSSQL
         //   SshDataSource.Type.ORACLE
         //   SshDataSource.Type.POSTGRESQL
         Connection con=null;
         ds.setUrl( SshDataSource.MYSQL, 3306, "test" );
         try{
            con = ds.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM test");
            while(rs.next()){
                System.out.println(QueryUtils.saveRowAsMap(rs));
            }
         }finally{
            con.close();
         }
         ds.close();
         
      }
}