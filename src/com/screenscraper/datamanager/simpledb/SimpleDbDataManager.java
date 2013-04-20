/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.simpledb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.screenscraper.datamanager.DataManager;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.DataWriter;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.skeleton.BasicDataManager;
import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author ryan
 */
public class SimpleDbDataManager extends BasicDataManager implements DataManager{


    AmazonSimpleDB sdb;
    private String schemaFile;
    private String namespace;

	/***
	 *
	 * @param accessKey
	 * @param secretKey
	 * @param schemaFile
	 */
    public SimpleDbDataManager(String accessKey, String secretKey, String schemaFile)
    {
        this.schemaFile=schemaFile;
        AWSCredentials creds = new BasicAWSCredentials(accessKey,secretKey);
        sdb = new AmazonSimpleDBClient(creds);
    }

    @Override
    public void buildSchemas()
    {
        try{
            SimpleDbSchemaBuilder sb = new SimpleDbSchemaBuilder(new File(schemaFile));
            schema = sb.getSchemas();
            createDomains(getDomainsInModel());
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void createDomains(Set<String> domains)
    {
        for(String domain:domains)
        {
            if(namespace!=null && !namespace.equals(""))
            {
                domain=namespace+"."+domain;
            }
            //sdb.createDomain(new CreateDomainRequest(domain));
        }
    }

    public void setNamespace(String namespace)
    {
        this.namespace=namespace;
    }
    public String getNamespace()
    {
        return namespace;
    }

    @Override
    protected DataWriter getNewDataWriter()
    {
        SimpleDbDataWriter dw = new SimpleDbDataWriter(this);
        return dw;
    }

    @Override
    public boolean flush()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Set<String> getDomainsInModel()
    {
        Set<String> domains = new HashSet<String>();
        String prefix = this.getNamespace();
        for(RelationalSchema s: schema.getRelationalSchemas())
        {
            if(((SimpleDbSchema)s).isDomainSchema()){
                String domain;
                if(prefix!=null && !prefix.isEmpty())
                    domain=prefix+"."+s.getName();
                else
                    domain=s.getName();
                domains.add(domain);
            }
        }
        return domains;
    }

    public AmazonSimpleDB getAmazonSimpleDbClient()
    {
        return sdb;
    }

    @Override
    public boolean commit(String schema)
    {
       // System.out.println("committing '"+schema+"'");
        boolean returnVal =  checkDataNode(schema);
        DataNode n = getCurrentDataNode(schema);
        connectTree(n);
        currentDataNodes.remove(schema);
        return returnVal;
    }



    @Override
    public boolean checkDataNode(String schema)
    {
        return true;
    }

    public boolean importSchemas(String file) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}