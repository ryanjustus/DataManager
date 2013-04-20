/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package test;

import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.simpledb.SimpleDbDataManager;
import com.screenscraper.datamanager.simpledb.SimpleDbDataWriter;
import com.screenscraper.datamanager.sql.*;
import java.io.UnsupportedEncodingException;
import java.util.*;
import org.apache.commons.dbcp.BasicDataSource;

/**
 *
 * @author ryan
 */
public class Test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args){
        try
        {
            testSimpleDbDm();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void deleteDomains(SimpleDbDataManager dm){
        ListDomainsResult domains = dm.getAmazonSimpleDbClient().listDomains();
        for(String domain: domains.getDomainNames()){
            DeleteDomainRequest req = new DeleteDomainRequest();
            req.setDomainName(domain);
            dm.getAmazonSimpleDbClient().deleteDomain(req);
        }
    }

    public static void testSimpleDbDm() throws UnsupportedEncodingException
    {
        SimpleDbDataManager dm = new SimpleDbDataManager("AKIAJKWGLQOU5WBH3WVA","jFSiB681MFvCHcvY1r3Ej3aFGbaF9AFeA7d6GBvb","schema_sample.xml");
        dm.buildSchemas();


        Set<String> domains = dm.getDomainsInModel();
        System.out.println("domains: ");
        for(String domain:domains)
            System.out.println("\t" + domain);

        for(RelationalSchema s : dm.getDatabaseSchema().getRelationalSchemas())
        {
            System.out.println(s);
        }

        //deleteDomains(dm);
        //add names
        generateSampleData(dm);

        
    }

    public static void generateSampleData(SimpleDbDataManager dm) throws UnsupportedEncodingException
    {
        for(int i=0;i<2;i++){
            Map m = new HashMap();
            m.put("fname", "fname " +i);
            m.put("lname", "lname "+ i);
            dm.addData("physician", m);

            for(int j=0;j<2;j++){      
                Map ma = new HashMap();
                ma.put("address", "address " +j);
                ma.put("zip", "1111" +j);
                dm.addData("office", ma);

                for(int k=0;k<2;k++) {
                    dm.addData("office_specialty","specialty", "spec " + k);
                    dm.addData("office_specialty", "board_certified", "true");
                    for(int l =0;l<4;l++){
                        dm.addData("sub_specialty","specialty","sub_spec " + l);
                        dm.commit("sub_specialty");
                    }
                    dm.commit("office_specialty");
                 }
                dm.commit("office");
            }
            for(int j=0;j<3;j++) {
                dm.addData("hospital", "hospital","hosp" +j);
                dm.commit("hospital");
            }
            dm.commit("physician");
            SimpleDbDataWriter dw = new SimpleDbDataWriter(dm);
            dm.setNamespace("testdata");
           // System.out.println("******TEST WRITE " + i + " *********");
            dw.setNamespace("testdata");
            dw.startWriting(null);
            dw.write(dm.getRoot());
            
            dm.clearAllData();
            AmazonSimpleDB sdb = dm.getAmazonSimpleDbClient();
            ListDomainsResult domains = sdb.listDomains();
            System.out.println("domains: " + domains.getDomainNames());
            GetAttributesRequest get = new GetAttributesRequest();
            get.setDomainName("testdata.office");
            Set<String> attrs = new HashSet<String>();
            attrs.add("address");
            attrs.add("zip");
            attrs.add("office_specialty");
            get.setAttributeNames(attrs);
            
            get.setItemName("tbzri9tsqtrw8q977q7dot0he504vgs");
            GetAttributesResult result = sdb.getAttributes(get);
            List<Attribute> attrsResult = result.getAttributes();
            System.out.println("testdata.office");
            for(Attribute attr: attrsResult){
                System.out.println(attr.getName()+ ":" + attr.getValue());
            }

            System.out.println("Selecting offices...");
            SelectRequest select = new SelectRequest();
            select.setSelectExpression("SELECT * FROM `testdata.office`");
            SelectResult rs = sdb.select(select);
            List<Item> items = rs.getItems();
            for(Item item: items){
                System.out.println(item.toString());
            }
        }
    }

    public static void testSqlDm() throws Exception
    {
         BasicDataSource ds = new BasicDataSource();
         ds.setDriverClassName( "com.mysql.jdbc.Driver" );
         ds.setUsername( "root" );
         ds.setPassword( "" );
         ds.setUrl("jdbc:mysql://localhost:3306/test");
        // Create Data Manager
        SqlDataManager dm = new SqlDataManager(ds);
        dm.buildSchemas();
        for(RelationalSchema s : dm.getDatabaseSchema().getRelationalSchemas())
        {
            System.out.println(s);
        }

       // generateSampleData(dm);
        dm.close();
    }
}
