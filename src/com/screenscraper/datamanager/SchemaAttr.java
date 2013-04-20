/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import java.util.Set;

/**
 *
 * @author ryan
 */
public interface SchemaAttr {
    Set<String> getOptions();
    String getDefaultOption();
}
