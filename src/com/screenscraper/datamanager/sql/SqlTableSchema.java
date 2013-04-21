/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.sql;

import com.screenscraper.datamanager.*;
import com.screenscraper.datamanager.skeleton.BasicSchema;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 *
 * @author ryan
 */
public class SqlTableSchema extends BasicSchema implements RelationalSchema{


    public enum SqlSchemaAttr implements SchemaAttr{
        columnName("columnName", null),
        sqlType("jcbcType", null),
        sqlTypeName("jdbcName", null),
        fkDeleteRule("fkDeleteRule", null);

        String name;
        Enum defaultOption;
        Set<String> options;
        SqlSchemaAttr(String name, Enum defaultOption, Enum...options){
            this.defaultOption=defaultOption;
            this.name=name;
            this.options=new HashSet();
            for(Enum option: options){
                this.options.add(option.name());
            }
        }

        Enum value;        

        @Override
        public String toString(){
            return name;
        }

        public Set<String> getOptions() {
            return options;
        }

        public String getDefaultOption() {
            return (defaultOption==null) ? null : defaultOption.name();
        }
    }

    /**
     * copy the schemas columns and attributes into a new schema
     * @param name
     * @param s
     */
    public SqlTableSchema(String name, RelationalSchema s)
    {
        super(name);
        Set<String> cols = s.getColumns();
        for(String column:cols)
        {
            this.columns.put(column, new HashMap<SchemaAttr,String>(s.getAttrs(column)));
        }
    }

    public SqlTableSchema(String name)
    {
        super(name);
    }

 
     @Override
    public Map<SchemaAttr,String> getDefaultAttrs(){
        Map sAttr = super.getDefaultAttrs();
        Map<SchemaAttr, String> sqlAttr = new HashMap<SchemaAttr, String>();
        for(SqlSchemaAttr attr: SqlSchemaAttr.values()){
            String dAttr = attr.getDefaultOption();
            if(dAttr!=null){
                sqlAttr.put(attr, dAttr);
            }
        }
        sAttr.putAll(sqlAttr);
        return sAttr;
    }

    /**
     * returns primary keys associated with the schema
     * @return Set of primary key columns names as Strings
     */
    public Set<String> getPrimaryKeyColumns()
    {
        SchemaKey primary = getPrimaryKey();
        return new HashSet<String>(primary.getColumns());
    }
}
