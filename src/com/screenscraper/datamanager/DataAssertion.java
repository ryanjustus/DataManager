/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.screenscraper.datamanager;

/**
 *
 * @author ryan
 */
public interface DataAssertion {
    /**
     * Validate data against a given DataNode
     * @param n Node to perform test on
     * @return 
     */
    boolean validateData(DataNode n);
    /**
     * Failure operation
     */
    void onFail();
    /**
     * Success operation
     */
    void onSuccess();
    /**
     * Return the schema that you want to perform the check on
     */
    public String getSchema();
}
