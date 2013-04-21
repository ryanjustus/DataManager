/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.screenscraper.datamanager.sql;

import com.screenscraper.datamanager.DatabaseSchema;
import com.screenscraper.datamanager.SchemaAttr;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author ryan
 */
public class SqlDatabaseSchema extends DatabaseSchema{
    
    public enum DatabaseSchemaAttr implements SchemaAttr{
        Vendor,
        MajorVersion,
        MinorVersion,
        DatabaseName,
        DatabaseIdentifierQuote,
        AutoIncrementSupport("yes","yes","no");       
        
        private DatabaseSchemaAttr(String defaultOption, String...options){
            this.defaultOption=defaultOption;
            this.options = new HashSet(Arrays.asList(options));
        }
        private DatabaseSchemaAttr(){
            this.defaultOption="";
            this.options = new HashSet();
        }
        private final Set<String> options;
        private final String defaultOption;
        public Set<String> getOptions() {
            return options;
        }

        public String getDefaultOption() {
            return defaultOption;
        }        
    }


    
}