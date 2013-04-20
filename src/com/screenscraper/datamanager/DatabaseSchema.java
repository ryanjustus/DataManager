/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import java.util.*;
/**
 * Contains a set of AbstractSchema of all the schemas contained by the DataManager
 * @author ryan
 */
public class DatabaseSchema{
    private final Set<RelationalSchema> schemas;
    private final Map<SchemaAttr, String> attrs;
    
    
    public DatabaseSchema()
    {
        schemas = new HashSet<RelationalSchema>();
        attrs = new HashMap<SchemaAttr, String>();
    }
    
    /**
     * Used for attributes that apply to the database as a whole
     * @param attr
     * @param value
     * @return 
     */
    public DatabaseSchema addAttr(SchemaAttr attr, String value){
        attrs.put(attr, value);
        return this;
    }
    
    public String getAttr(SchemaAttr attr){
        return attrs.get(attr);
    }
    
    /**
     * adds a schema to Schemas
     * @param s
     */
    public DatabaseSchema addRelationalSchema(RelationalSchema s)
    {
        schemas.add(s);
        return this;
    }
    /**
     *
     * @param name
     * @return schema corresponding to name, or null
     */
    public RelationalSchema getRelationalSchema(String name)
    {
        if(name==null){
            throw new NullPointerException("passed in null on getRelationalSchema");
        }
        for(RelationalSchema s:schemas)
        {
            if(s.getName().toLowerCase().equals(name.toLowerCase()))
                return s;
        }
        throw new IllegalArgumentException("unknown Schema " + name);
    }
    
    public boolean hasRelationalSchema(String name){
        if(name==null)
            return false;
        for(RelationalSchema s:schemas)
        {
            if(s.getName().toLowerCase().equals(name.toLowerCase()))
                return true;
        }       
        return false;
    }
    
    public Set<RelationalSchema> getRelationalSchemas()
    {
        return new HashSet(schemas);
    }
}


