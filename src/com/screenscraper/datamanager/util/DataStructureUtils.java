/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.util;

import com.screenscraper.datamanager.DataObject;
import java.util.*;

/**
 *
 * @author ryan
 */
public class DataStructureUtils {


    public static boolean containsAllIgnoreCase(Set<String> input,Set<String> items){
        for(String item: input){
            if(!containsIgnoreCase(item, items))
                return false;
        }
        return true;
    }

    public static String getCollectionCase(String item, Collection<String> items){
        for(String i:items){
            if(item.equalsIgnoreCase(i))
                return i;
        }
        return null;
    }

    public static boolean containsIgnoreCase(String item, Collection<String> items){
        for(String i:items){
            if(item.equalsIgnoreCase(i))
                return true;
        }
        return false;
    }


    public static String join( String delimiter, Object[] array )
    {
        Iterator<Object> itr = Arrays.asList(array).iterator();
        StringBuilder sb = new StringBuilder();
        while(itr.hasNext()){
            sb.append(itr.next());
            if(itr.hasNext()){
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public static String join( String delimiter, Collection set )
    {
        Iterator itr = set.iterator();
        StringBuilder sb = new StringBuilder();
        while(itr.hasNext()){
            sb.append(itr.next());
            if(itr.hasNext()){
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

     /*
     * returns a Map with all the data that needs to be merged
     */
    public static void mergeIgnoreCase(Map<String,DataObject> existingData, Map<String,DataObject> newData)
    {
        for(String key: newData.keySet())
        {
            if(!containsIgnoreCase(key, existingData.keySet()))
                existingData.put(key, newData.get(key));
        }
    }

}
