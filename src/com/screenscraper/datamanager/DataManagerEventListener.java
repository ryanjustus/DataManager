/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

/**
 * Interface for users to add their own custom events on certain data operations
 * @author ryan
 */
public interface DataManagerEventListener {
    public void handleEvent(DataManagerEvent event);
}
