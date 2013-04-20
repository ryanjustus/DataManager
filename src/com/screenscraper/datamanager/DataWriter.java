/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

/**
 * The DataWriter is responsible for writing out the data contained in the
 * DataManager.  The DataWriter should be called by the DataManager and not
 * called directly.
 * @author ryan
 */
public interface DataWriter {

    /**
     * writes the nodes
     * @param node
     * @throws java.lang.Exception
     */
    public boolean write(RootNode node);

    /**
     * starts up the writing, this is not always required but may be depending on the output
     * The purpose is to write out file headers, and get things going in general
     * @param startUp
     */
    public void startWriting(String startUp);
    /**
     * finishes up the writing, this is not always required but may be depending on the output
     * The purpose is to write out file footers, and get things cleaned up in general
     * @param finishUp
     */
    public void finishWriting(String finishUp);

    /**
     * Closes any connections and frees up any system resources/files
     */
    public void close();
}