/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import java.util.EventObject;
import java.util.Set;

/**
 * 
 * @author ryan
 */
public class DataManagerEvent extends EventObject{

    private DataNode n;

    /**
     *
     * @param o
     * @param n
     */
    public DataManagerEvent(Object source,DataNode n)
    {
        super(source);
        this.n=n;
    }
 
    /**
     * Retrieve the DataNode associated with the DataManagerEvent
     * @return 
     */
    public DataNode getDataNode(){
        return n;
    }
}