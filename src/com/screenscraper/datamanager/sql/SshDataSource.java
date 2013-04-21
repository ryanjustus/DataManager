package com.screenscraper.datamanager.sql;

import ch.ethz.ssh2.Connection;
import com.screenscraper.datamanager.sql.util.PSBuilder;
import java.io.*;
import java.net.ServerSocket;
import java.sql.SQLException;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SshDataSource extends BasicDataSource {

    public static final String MYSQL = "MySql";
    public static final String MSSQL = "MsSql";
    public static final String ORACLE = "Oracle";
    public static final String POSTGRESQL = "PostgreSql";
    public String remoteUrl = "localhost";
    private Connection sshCon;
    private static Log log = LogFactory.getLog(PSBuilder.class);
    /**
     * Default using ssh port 22
     * @param sshUser Ex: user@remotehost.com
     * @param password
     */
    public SshDataSource(String sshUser, String password)
    {
       super();
       initWithPass(sshUser, 22, password);
    }
    /**
     * Creates a BasicDataSource that uses an ssh tunnel to connect to the database
     * @param sshUser
     * @param sshPort
     * @param password
     */
    public SshDataSource(String sshUser, int sshPort, String password)
    {
        super();
        initWithPass(sshUser, sshPort, password);
    }

    /**
     * Default using ssh port 22
     * @param sshUser Ex: user@remotehost.com
     * @param key
     * @throws FileNotFoundException
     */
    public SshDataSource(String sshUser, File key) throws FileNotFoundException
    {
        super();
        initWithKey(sshUser, 22, key);
    }

    /**
     * Creates a BasicDataSource that uses an ssh tunnel to connect to the database
     * @param sshUser
     * @param sshPort
     * @param key
     * @throws FileNotFoundException
     */

    public SshDataSource(String sshUser, int sshPort,File key) throws FileNotFoundException
    {
        super();
        initWithKey(sshUser, sshPort, key);
    }

    private void initWithPass(String sshUser, int sshPort, String password)
    {
        String[] user = sshUser.split("@");
        String uname = "";
        String host = "";
        if(user.length==2)
        {
            uname = user[0];
            host = user[1];
        }else
            throw new IllegalArgumentException("sshUser must be of the form user@host");
        if(sshPort<1 || sshPort>65535)
            throw new IllegalArgumentException("invalid ssh port");
        log.info("connecting to "+host+"...");
        sshCon = new Connection(host,sshPort);        
        try {
            sshCon.connect();
            sshCon.authenticateWithPassword(uname, password);   
            log.info("connected");
        } catch (IOException ex) {
            
            log.error("Error establishing ssh connection", ex);
        }
    }

    private void initWithKey(String sshUser, int sshPort, File key) throws FileNotFoundException
    {
        String[] user = sshUser.split("@");
        String uname = "";
        String host = "";
        if(user.length==2)
        {
            uname = user[0];
            host = user[1];
        }
        else
            throw new IllegalArgumentException("sshUser must be of the form user@host");
        if(sshPort<1 || sshPort>65535)
            throw new IllegalArgumentException("invalid ssh port");
        try {
            sshCon = new Connection(host,sshPort);
            sshCon.authenticateWithPublicKey(uname, key, password);
        } catch (IOException ex) {
            log.error("Error establishing ssh connection", ex);
        }

    }


    @Override
    public void close() throws SQLException
    {
        super.close();
        sshCon.close();
    }

    /**
     * Use this instead of setUrl(String url).  Makes an sql connection that is
     * forwarded through the ssh connection to the remote sql server
     * @param sqlDatabaseType SshDataSource.MYSQL, SshDataSource.MSSQL, SshDataSource.ORACLE, or SshDataSource.POSTRESQL
     * @param sqlServerPort listening port of remote sql server
     * @param databaseName name of the database you want to connect to or null
     */
    public void setUrl(String sqlDatabaseType, int sqlServerPort, String databaseName)
    {
        if(databaseName == null)
            databaseName = "";
        try {
            int localPort = getFreePort();
            
            sshCon.createLocalPortForwarder(localPort, remoteUrl, sqlServerPort);
            String sqlUrl = SshDataSource.getSqlConnectionPath(sqlDatabaseType,localPort, databaseName);
            super.setUrl(sqlUrl);
        } catch (IOException ex) {
            log.error("Error establishing port forwarding", ex);
        }
    }
    
        /**
     * Use this instead of setUrl(String url).  Makes an sql connection that is
     * forwarded through the ssh connection to the remote sql server
     * @param sqlDatabaseType SshDataSource.MYSQL, SshDataSource.MSSQL, SshDataSource.ORACLE, or SshDataSource.POSTRESQL
     * @param sqlServerPort listening port of remote sql server
     * @param databaseName name of the database you want to connect to or null
     */
    public void setUrl(String sqlDatabaseType, String url, int sqlServerPort, String databaseName)
    {
        if(databaseName == null)
            databaseName = "";
        try {
            int localPort = getFreePort();
            
            sshCon.createLocalPortForwarder(localPort, remoteUrl, sqlServerPort);
            String sqlUrl = SshDataSource.getSqlConnectionPath(sqlDatabaseType,url,localPort, databaseName);
            super.setUrl(sqlUrl);
        } catch (IOException ex) {
            log.error("Error establishing port forwarding", ex);
        }
    }
    
    /**
     * Sets the remote url you wish to connect to
     * @param url, default localhost
     */
    public void setRemoteUrl(String url){
        this.remoteUrl=url;
    }


    @Override
    public void setUrl(String url)
    {
        throw new UnsupportedOperationException("use SshDataSource.setUrl(String sqlDatabaseType, int sqlServerPort, String databaseName)");
    }

    private static String getSqlConnectionPath(String type, int port, String database)
    {
        String hostpath = "localhost";
        String url ="";
        if(type.equals(SshDataSource.MYSQL))
            url =  "jdbc:mysql://" + hostpath +":"+port + "/" + database;
        else if(type.equals(SshDataSource.MSSQL))
            url = "jdbc:jtds:sqlserver://" + hostpath+":" + "/" + database;
        else if(type.equals(SshDataSource.ORACLE))
            url = "jdbc:oracle:thin:@"+hostpath+ ":" + port+":"+database;
        else if(type.equals(SshDataSource.POSTGRESQL))
            url = "jdbc:postgresql://" + hostpath + ":" + port + "/" + database;
        else
            throw new IllegalArgumentException("unknown database type " + type);
        return url;
    }
    
    private static String getSqlConnectionPath(String type, String url, int port, String database)
    {
        String hostpath = "localhost";
        if(type.equals(SshDataSource.MYSQL))
            url =  "jdbc:mysql://" + hostpath +":"+port + "/" + database;
        else if(type.equals(SshDataSource.MSSQL))
            url = "jdbc:jtds:sqlserver://" + hostpath+":" + "/" + database;
        else if(type.equals(SshDataSource.ORACLE))
            url = "jdbc:oracle:thin:@"+hostpath+ ":" + port+":"+database;
        else if(type.equals(SshDataSource.POSTGRESQL))
            url = "jdbc:postgresql://" + hostpath + ":" + port + "/" + database;
        else
            throw new IllegalArgumentException("unknown database type " + type);
        return url;
    }


    public static int getFreePort() {
        boolean free=false;
        int port =0;
        while(!free)
        {
            port = (int)(1024+Math.random()*(65535-1024));
            try
            {
                ServerSocket socket = new ServerSocket(port);
                socket.close();
                free=true;
            }
            catch (IOException ex)
            {
            }
        }
        return port;
    }
}