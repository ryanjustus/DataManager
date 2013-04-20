The purpose of the DataManager is to make it easier to save data from some arbitrary source into a database.  It is not an ORM and only provides limited support for querying the data.  

It was developed for use at Screen-scraper.com in order speed up development for web scraping, specifially in saving the data to the database.  

A common difficulty we would have is if a client wanted an additional datapoint saved from a site then we would have to dig through all of our code and find and modify the relevant sql statements.  With the datamanager we just add the additional column to the table and add the datapoint to a map of data we are already saving and the datamanager takes care of writing the sql for the insert/updates.  Additionally it greatly simplifies the process of connecting all the rows together with foreign keys.

It has been used extensively internally at screen-scraper.com for saving scraped data to MySQL, Microsoft SQL Server, SQLite, Hypersonic, and PostreSQL.

Basic usage example:

```java
    
    // Connect to database using a org.apache.commons.dbcp.BasicDataSource
    BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName( "com.mysql.jdbc.Driver" );
    ds.setUsername( username );
    ds.setPassword( password );
    ds.setUrl( "jdbc:mysql://" + hostpath + ":" + port + "/" + database + "?" + dbparams );
    ds.setMaxActive( 10);

    /**
    * Create Data Manager, this example for creating a connection to an sql rdbms
    * There is also an option to use Amazon SimpleDB as a backend and more backends 
    * may be supported in the future
    **/
    SqlDataManager dm = new SqlDataManager( ds );
    dm.setLoggingLevel( org.apache.log4j.Level.DEBUG ); //Debug mode will log out the sql queries generated
  
    /**
    * Build schemas reads the database structure from the target database so that the library
    * can build the appropriate queries and add in foreign keys
    **/
    dm.buildSchemas();

    /**
    * The following settings described how data updates are dealt with in cases where the data we are 
    * trying to write matches a unique key for existing data.
    * Tble Summary of what each setting does
    *                                 UPDATE
    *                      false                        true
    *                 --------------------------------------------------------------
    *           false |  No updates are performed  | Only NOT NULL database values |
    *                 | (All queries INSERT IGNORE)| are updated                   |
    *  MERGE          |------------------------------------------------------------| 
    *           true  | Only NULL database values  | All existing data is updated, |
    *                 | are updated.               | but is never updated to NULL  |
    *                 --------------------------------------------------------------
    **/
    
    dm.setGlobalUpdateEnabled( true );
    dm.setGlobalMergeEnabled( true );

    /**
    * We are now ready to save data, for this example we will save a person and a couple of addresses
    * Related to them.  This assumes your database has a 'person' table and an 'address' table.
    *  table, column, value 
    **/
    dm.addData("person","name", "John Doe");

    // Or we can accept a Map of values
    Map data = new HashMap();
    data.put("address", "123 Main Street");
    data.put("city", "Townsvilleburgton");
    data.put("state", "WV");
    dm.addData("address", data);

    /**
    * Calling commit closes off the record for writing so the next time dm.addData is called
    * for the table it will correspond to a new database row.  This does not cause the actuall
    * write to happen yet, however.
    *//
    dm.commit("address");

    //Add another address for the same person, it is case insensitve for table/column names
    dm.addData("ADDRESS", "address", "457 Minor Dr.");
    dm.addDAta("address", "City", "Megacity");
    dm.addData("address", "STATE", "NC");
    dm.commit("address");

    /**
    * We now have a person and two addresses.  The library knows the structure of the database and 
    * that we have a foreign key relationship between 'person' and 'address' so it will connect them 
    * in the database.  For this to happen all of the children of a row need to be added before the parent
    * is committed.  Since we added the children we can now commit 'person'.
    **/
    dm.commit("person");
    
    /**
    * We have all of our data added, so lets write it out by calling flush().  If we wanted we could add     
    * multiple 'person' entries and write them all out at once, or we could write them out once we get a
    * complete record. 
    **/
    dm.flush(); 
    
    //Calling dm.close() closes the underlying BasicDataSource
    dm.close();
```
That is it for the basic usage. 
Other more complex activities include setting up lookup queries for complex relational data, saving many-to-many relational data, tunneling to mysql via ssh, multithreaded write support, and setting up event callbacks.

I will try to get some more complex examples added in the future.
Some more complex examples soon.
   
    

 
