/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.simpledb;

import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.google.gson.Gson;
import com.screenscraper.datamanager.DataNode;
import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.DataWriter;
import com.screenscraper.datamanager.RootNode;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.SchemaKey;
import com.screenscraper.datamanager.simpledb.SimpleDbSchema.SimpleDbSchemaAttrs;
import com.screenscraper.datamanager.skeleton.BasicSchemaAttr;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
/**
 *
 * @author ryan
 */
public class SimpleDbDataWriter implements DataWriter{
    
    private SimpleDbDataManager dm;
    private String namespace;
    private final Gson gson = new Gson();
    Map<String, BatchPutAttributesRequest> domainRequests;

    public SimpleDbDataWriter(SimpleDbDataManager dm)
    {        
        this.namespace=dm.getNamespace();
        this.dm=dm;
    }

    @Override
    public void startWriting(String startUp) {
        System.out.println("executing startWriting");
        Set<String> domains = dm.getDomainsInModel();
        ListDomainsResult domainsResponse = dm.sdb.listDomains();
        List<String> existingDomains = domainsResponse.getDomainNames();
        System.out.println("existingDomains: " + existingDomains);
        for(String domain: domains){
            if(!existingDomains.contains(domain)){
                System.out.println("creating domain: " +domain);
                CreateDomainRequest create = new CreateDomainRequest();
                create.setDomainName(domain);
                dm.sdb.createDomain(create);
            }
        }
    }

    @Override
    public void finishWriting(String finishUp) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean write(RootNode root)
    {
        System.out.println("writing data");
        domainRequests = new HashMap<String, BatchPutAttributesRequest>();
        for(DataNode n: root.getChildren()) {
            addAutoData(n,new HashSet<DataNode>());
            Map<String,List<DataNode>> data = new HashMap<String,List<DataNode>>();
            try{
            flatten(n, data);
            }catch(UnsupportedEncodingException e){
                System.err.print(e);
            }
            for(Entry<String,List<DataNode>> e1: data.entrySet()) {
                String table = e1.getKey();
                if(namespace!=null && !namespace.equals("")) {
                    table=namespace+"."+table;
                }
                BatchPutAttributesRequest req = getBatchRequest(table);
                List<ReplaceableItem> items = new ArrayList<ReplaceableItem>();                
                for(DataNode node: e1.getValue()){
                    System.out.println(table);
                   // SimpleDbSchema sT = (SimpleDbSchema)n.getSchema();
                    //Set<String> keySet = node.getSchema().getKeys();
                    SchemaKey key = node.getSchema().getPrimaryKey();
                    if(key==null)
                        throw new IllegalStateException("Schema " + node.getSchema().getName() + " doesn't have a key defined");
                    ReplaceableItem r = new ReplaceableItem();
                    r.setName(String.valueOf(node.getValue(key.getColumns().get(0)).getObject()));
                    Collection<ReplaceableAttribute> attrs = new HashSet<ReplaceableAttribute>();
                    
                    for(Entry<String,Object>entry: node.getObjectMap().entrySet()) {
                        if(entry.getKey().equals(key))
                            continue;
                        ReplaceableAttribute attr = new ReplaceableAttribute();
                        attr.setName(entry.getKey());
                        attr.setValue(String.valueOf(entry.getValue()));
                        attrs.add(attr);
                        System.out.println("\t"+ entry.getKey()+": "+entry.getValue());
                    }
                    for(DataNode c: node.getChildren()) {
                        SimpleDbSchema s = (SimpleDbSchema)c.getSchema();
                        if(s.isSingleValueSchema()) {
                            ReplaceableAttribute attr = new ReplaceableAttribute();
                            attr.setName(s.getName());
                            attr.setValue(String.valueOf(c.getValue(s.getName())));
                            attrs.add(attr);
                            System.out.println("\t"+s.getName() +": " +c.getValue(s.getName()));
                        }else if(s.isSubDocumentSchema()){
                            SchemaForeignKey fk = s.getForeignKeys(e1.getKey()).iterator().next();
                            Object o = c.getValue(fk.getKeyMap().keySet().iterator().next()).getObject();
                            String value = gson.toJson(o);
                            ReplaceableAttribute attr = new ReplaceableAttribute();
                            attr.setName(s.getName());
                            attr.setValue(value);
                            attrs.add(attr);
                            System.out.println("\t"+s.getName() +": " +value);
                        }
                    }
                    r.setAttributes(attrs);
                    items.add(r);
                }
                req.setItems(items);
            }
        }
        //write the data out to Amazon SimpleDb
        for(BatchPutAttributesRequest req : domainRequests.values()){
            System.out.println("writing ... " + req.toString());
            dm.sdb.batchPutAttributes(req);
        }
        return true;
    }

    private BatchPutAttributesRequest getBatchRequest(String domain){
        BatchPutAttributesRequest request =   domainRequests.get(domain);
        if(request==null){
            request = new BatchPutAttributesRequest();
            request.setDomainName(domain);
            domainRequests.put(domain, request);
        }
        return request;
    }

    /**
     * Prints the data to standard out for testing
     * @param root
     * @throws UnsupportedEncodingException
     */
    public void testWrite(RootNode root) throws UnsupportedEncodingException
    {
        System.out.println("writing data");
        
        for(DataNode n: root.getChildren()) {

            addAutoData(n,new HashSet<DataNode>());
            Map<String,List<DataNode>> data = new HashMap<String,List<DataNode>>();
          //  System.out.println("flattening: " + n.getSchema().getName());
            flatten(n, data);
            for(Entry<String,List<DataNode>> e1: data.entrySet()) {
                String table = e1.getKey();
                if(namespace!=null && !namespace.equals("")) {
                    table=namespace+"."+table;
                }                
                for(DataNode node: e1.getValue()){
                    System.out.println(table);
                    for(Entry<String,Object>entry: node.getObjectMap().entrySet()) {
                        System.out.println("\t"+ entry.getKey()+": "+entry.getValue());
                    }
                    for(DataNode c: node.getChildren()) {
                        SimpleDbSchema s = (SimpleDbSchema)c.getSchema();
                        if(s.isSingleValueSchema()) {
                            System.out.println("\t"+s.getName() +": " +c.getValue(s.getName()));
                        }else if(s.isSubDocumentSchema()){
                             SchemaForeignKey fk = s.getForeignKeys(e1.getKey()).iterator().next();
                             Object o = c.getValue(fk.getKeyMap().keySet().iterator().next()).getObject();
                             System.out.println("\t"+s.getName() +": " +gson.toJson(o));
                        }
                    }
                }                
            }
        }
    }

   
    /**
     * Saves foreign key data from patents into children DataNodes
     * as well as saving the defaultValues and computed values (hash,unique,cat)
     * into the nodes.  For relationships that use a separate domain we need to save
     * the foreign reference (domain,key) into the DataNode
     *
     * This is a recursive function which traverses the entire DataNode tree.
     * The starting point is each child of RootNode with an empty Set.
     * @param node
     * @param visited
     */
    private void addAutoData(DataNode node, Set<DataNode> visited)
    {
        //this is to make sure that all of the nodes parents uid information is added
        //if it is not then the a hashed auto data will not necessarily be correct
        for (DataNode parent : node.getParents()) {
            if (!visited.contains(parent)) {
                return;
            }
        }
        if(visited.contains(node))
        {
           return;
        }
        visited.add(node);

        //add the default data
        for(String column: node.getSchema().getColumns()) {
            String defaultVal = node.getSchema().getAttr(column, BasicSchemaAttr.defaultValue);
            if(defaultVal!=null && !defaultVal.equals("")){
                if(node.getValue(column)==null || node.getValue(column).getObject()==null || node.getValue(column).getObject().equals(""))
                    try{
                        node.addData(column, defaultVal);
                    }catch(UnsupportedEncodingException e){e.printStackTrace();}
            }
        }

        //add the autogen key data
        Set<String> columns = node.getSchema().getAutoGeneratedColumns();
        for(String column: columns){
            try{

                String cc = node.getSchema().getAttr(column, SimpleDbSchema.SimpleDbSchemaAttrs.autoColumns);
                Set columnsToUse = null;
                if(cc!=null){
                   columnsToUse = new HashSet();
                   columnsToUse.addAll(Arrays.asList(cc.split(",")));
                }

                DataObject value = node.getValue(column);
                if(value==null || value.getObject().equals("")) {
                    //add in the autogenerated values
                    String type = node.getSchema().getAttr(column, SimpleDbSchemaAttrs.autoGenType);
                    type = (type==null) ? "" : type;
                    if(type.equals("unique"))
                        node.addData(column, SimpleDbDataManager.generateUID(column));
                    else if(type.equals("hash"))
                        node.addData(column, SimpleDbDataManager.hashData(node, columnsToUse));
                    else if(type.equals("cat")){
                        node.addData(column, SimpleDbDataManager.catData(node, columnsToUse));
                    }
                }
            }catch(NoSuchAlgorithmException e){e.printStackTrace();}
             catch(UnsupportedEncodingException e){e.printStackTrace();}
        }

        //add fk relationsihp data (contained in children nodes)
        for(DataNode c: node.getChildren()) {
            String ns = dm.getNamespace();
            String domain;
            if(ns!=null && !ns.isEmpty())
                domain=ns+"."+c.getSchema().getName();
            else
                domain=c.getSchema().getName();
            if(dm.getDomainsInModel().contains(domain))
            {
                Set<SchemaForeignKey> fks = c.getSchema().getForeignKeys();
                for(SchemaForeignKey fk : fks) {
                    String table = fk.getParentSchemaName();
                    if(table.equals(node.getSchema().getName())){
                        int i=0;
                        for(String column : fk.getKeyMap().keySet()){
                            c.addData(column, node.getValue(fk.getKeyMap().get(column)));
                            i++;
                        }
                    }
                }
            }
            addAutoData(c,visited);
        }
    }

    /**
     * Flattens the data in to non-explicitly relational tables.
     * SimpleDb doesn't support sub-documents so this flattens the data according to
     * the rules defined in the schema about how simulate the functionality, whether by
     * separate domains, Single values or embedded JSON text.  This is a recursive function so the
     * top level should accept each of the children of RootNode and an empty map.
     * The flattened data is saved in this empty map, which will be used to generate the
     * actual SimpleDB calls.
     *
     * In SimpleDb terminology the Map data corresponds to <domain<name, List<attributes>>
     *
     * Note that if you make a circular reference
     * sub-documents in your schema definition that this method will stack overflow all up
     * in your face. So don't do something like parent -> child -> parent as it will keep nesting.
     *
     * @param n
     * @param top
     * @return
     */
    private SortedMap<String,Object> flatten(DataNode n, Map<String,List<DataNode>>top) throws UnsupportedEncodingException
    {
        //top level schema
        if(((SimpleDbSchema)n.getSchema()).isDomainSchema()) {
            List<DataNode> l = top.get(n.getSchema().getName());
            if(l==null)
            {
                l = new ArrayList<DataNode>();
                top.put(n.getSchema().getName(), l);
            }
            l.add(n);

            for(DataNode c: n.getChildren()) {
                flatten(c,top);
            }
            return null;
        }
        //Json schema, flatten data/children json data into fk column(s)
        if(((SimpleDbSchema)n.getSchema()).isSubDocumentSchema())
        {
            SortedMap<String,Object>data = new TreeMap(n.getObjectMap());
            for(DataNode c: n.getChildren()){
                SortedMap<String,Object> cData = flatten(c,top);
                if(cData!=null && !cData.isEmpty()){
                    SortedSet l = (SortedSet)data.get(c.getSchema().getName());
                    if(l==null){
                        l = new TreeSet(new Comparator(){
                            public int compare(Object o1, Object o2) {
                                return gson.toJson(o1).compareTo(gson.toJson(o2));
                            }
                        });
                        data.put(c.getSchema().getName(), l);
                    }
                    l.add(cData);
                }
            }
            for(DataNode p: n.getParents())
            {
                SimpleDbSchema s = (SimpleDbSchema)p.getSchema();
                if(s.isDomainSchema())
                {
                    Set<SchemaForeignKey> keys = ((SimpleDbSchema)n.getSchema()).getForeignKeys(p.getSchema().getName());
                    String fkColumn = keys.iterator().next().getKeyMap().keySet().iterator().next();
                    n.addData(fkColumn, data);
                }
            }            
            return data;
        }
        return null;
    }


    private Map<String,List<ReplaceableItem>> generateStatements(Map<String,Map<String,List<Object>>> inputData)
    {
        Map<String,List<ReplaceableItem>> stmts = new HashMap<String,List<ReplaceableItem>>();
        for(Entry<String,Map<String,List<Object>>> entry : inputData.entrySet())
        {
            String key = entry.getKey();
//            for(Entry<String,Object> entry2: entry.getValue().entrySet()) {}
        }
        return stmts;
    }

    /**
     * Constructs JSON representation of a Map
     * @param objectMap
     * @return
     */
    public static String buildJSON(Map<String,Object> objectMap)
    {
        Gson gson = new Gson();
        String json = gson.toJson(objectMap);
        return json;
    }

    public void setNamespace(String namespace)
    {
        this.namespace=namespace;
    }
}