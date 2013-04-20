/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import com.screenscraper.datamanager.util.DataStructureUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * This class is immutable
 * @author ryan
 */
public class SchemaKey {

   public enum Type{
      primary,
      unique,
      normal
   };

   private List<String> columns;
   private Type type;
   private String name;
   
   public SchemaKey(String keyName, Type type, List<String> columns){
        this.name=keyName;
        this.type=type;
        this.columns = new ArrayList<String>();
        for(String column: columns){
            if(this.columns.contains(column)){
                throw new IllegalArgumentException("duplicate column in key: "+column);
            }
            this.columns.add(column);
        }
   }

   public List<String> getColumns(){
       return new ArrayList<String>(columns);
   }

   public Type getType(){
       return type;
   }

   public String getName(){
       return name;
   }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + (this.columns != null ? this.columns.hashCode() : 0);
        hash = 71 * hash + (this.type != null ? this.type.hashCode() : 0);
        hash = 71 * hash + (this.name != null ? this.name.hashCode() : 0);
        return hash;
    }

    @Override
   public boolean equals(Object o){
       if(o instanceof SchemaKey){
           return o.hashCode()==hashCode();
       }
       return false;
   }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder("SchemaKey: '");
        sb.append(type.name()).append("': Name: '").append(name).append("', columns:[");
        sb.append(DataStructureUtils.join(",", columns));
        sb.append("]");
        return sb.toString();
    }

}