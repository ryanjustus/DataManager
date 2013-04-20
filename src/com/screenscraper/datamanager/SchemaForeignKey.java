/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import com.screenscraper.datamanager.util.DataStructureUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * immutable class
 * @author ryan
 */
public class SchemaForeignKey{
    String parentSchema;
    String name;
    Map<String,String> keyMap;
   

    public SchemaForeignKey(String keyName, String parentSchemaName, Map<String,String> keyMap){
        if(keyMap==null){
            throw new IllegalArgumentException("keyMap is null");
        }
        this.name=keyName;
        //think about adding an invariant so schemaName has to be in schemas
        this.parentSchema=parentSchemaName;
        this.keyMap = new HashMap<String,String>();
        for(Entry<String,String> entry: keyMap.entrySet()){
            if(this.keyMap.containsValue(entry.getValue())){
                throw new IllegalArgumentException(entry.getValue()+" parent column used more than once, key must be one-to-one");
            }
            this.keyMap.put(entry.getKey(), entry.getValue());
        }
    }

    public Map<String,String> getKeyMap(){
        return new HashMap(this.keyMap);
    }

    public String getParentSchemaName(){
        return parentSchema;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof SchemaForeignKey){
           return o.hashCode()==hashCode();
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 79 * hash + (this.parentSchema != null ? this.parentSchema.hashCode() : 0);
        hash = 79 * hash + (this.name != null ? this.name.hashCode() : 0);
        hash = 79 * hash + (this.keyMap != null ? this.keyMap.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString(){
       StringBuilder sb = new StringBuilder(name);
       sb.append(":{");
       for(Entry<String,String> entry: keyMap.entrySet()){
           sb.append(entry.getKey()).append("->").append(parentSchema).append(".").append(entry.getValue());
       }
       sb.append("}");
       return sb.toString();
    }
}