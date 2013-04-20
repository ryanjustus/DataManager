/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager.simpledb;

import com.screenscraper.datamanager.DataObject;
import com.screenscraper.datamanager.RelationalSchema;
import com.screenscraper.datamanager.SchemaAttr;
import com.screenscraper.datamanager.SchemaForeignKey;
import com.screenscraper.datamanager.SchemaKey;
import com.screenscraper.datamanager.simpledb.SimpleDbSchema.SimpleDbSchemaAttrs.Option;
import com.screenscraper.datamanager.skeleton.BasicSchema;
import com.screenscraper.datamanager.skeleton.BasicSchemaAttr;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author ryan
 */
public class SimpleDbSchema extends BasicSchema implements RelationalSchema{

    private static Log log = LogFactory.getLog(SimpleDbSchema.class);

    public enum SimpleDbSchemaAttrs implements SchemaAttr{
        typeName("typeName", null),
        autoIncrement("auto increment", BasicSchemaAttr.Option.no, BasicSchemaAttr.Option.no, BasicSchemaAttr.Option.yes),
        subFormat("fkFormat", null, Option.json, Option.domain, Option.singleValue),
        autoGenType("autoType", null, Option.unique, Option.cat, Option.hash),
        autoColumns("autoColumns", null);

        public enum Option{
                domain("domain"),
                json("json"),
                singleValue("singleValue"),
                hash("hash"),
                cat("cat"),
                unique("unique");
            ;
            private String val;
            Option(String option){
                this.val=option;
            }

            public boolean isEqual(String option){
                if(val==null){
                    return (option==null);
                }
                return val.equals(option);
            }

            public boolean isEqual(Enum option){
                return this.isEqual(option.toString());
            }

            public static SchemaAttr fromString(String attr){
                try{
                    return BasicSchemaAttr.valueOf(attr);
                }catch(IllegalArgumentException e){
                    log.warn("attemping to add uknown attribute: " + attr + ", returning null");
                }
                return null;
            }

            @Override
            public String toString(){
                return val;
            }
        }

        String name;
        Set<Enum> options;
        Enum defaultOption;
        
        private SimpleDbSchemaAttrs(String name, Enum defaultOption, Enum...options){
            this.options = new HashSet<Enum>(Arrays.asList(options));
            this.defaultOption=defaultOption;
            this.name=name;
        }

        public String getDefaultOption() {
            return (defaultOption==null) ? null : defaultOption.name();
        }

        @Override
        public String toString()
        {
            return name;
        }

        public Set<String> getOptions()
        {
            Set<String> retStr = new HashSet<String>();
            for(Enum o: options){
                retStr.add(o.toString());
            }
            return retStr;
        }
    }

    public SimpleDbSchema(String name)
    {
        super(name);
    }

    boolean isSubFormatOfType(SimpleDbSchemaAttrs.Option option){
       Set<SchemaForeignKey> fks = this.getForeignKeys();
       if(fks.isEmpty() && option.isEqual(SimpleDbSchemaAttrs.Option.domain))
           return true;
       boolean ret = false;
        for(SchemaForeignKey fk: fks){
            String column = fk.getKeyMap().keySet().iterator().next();
            if(option.isEqual(getAttr(column, SimpleDbSchemaAttrs.subFormat))){
                ret=true;
                break;
            }
        }
        return ret;
    }

    boolean isValid(){
        boolean retDom= false;
        boolean retSub = false;
        boolean retSin = false;

        if(isDomainSchema() && !getKeys().isEmpty())
            retDom=true;
        if(isSingleValueSchema() && !getForeignKeys().isEmpty())
            retSub = true;
        if(isSubDocumentSchema() && !getForeignKeys().isEmpty())
            retSin = true;
        return (retDom && retSub && retSin);
    }

    private boolean isEmbeddedType(String type){
        if(SimpleDbSchemaAttrs.Option.json.isEqual(type))
           return true;
        else if(SimpleDbSchemaAttrs.Option.singleValue.isEqual(type))
            return true;
        return false;
    }

    public boolean isDomainSchema()
    {
        return isSubFormatOfType(SimpleDbSchemaAttrs.Option.domain);
    }

    public boolean isSubDocumentSchema()
    {
        return isSubFormatOfType(SimpleDbSchemaAttrs.Option.json);
    }

   
    public boolean isSingleValueSchema()
    {
        return this.isSubFormatOfType(SimpleDbSchemaAttrs.Option.singleValue);
    }

    public void addRelationship(String column, RelationalSchema parent, String type){
        if(!SimpleDbSchemaAttrs.subFormat.getOptions().contains(type)){
            throw new IllegalArgumentException(type + " not valid relationship type, must be in " + SimpleDbSchemaAttrs.subFormat.getOptions());
        }
        //System.out.println("setting "+ getName() + " as subFormat "+ type);
        this.addAttr(column, SimpleDbSchemaAttrs.subFormat, type);
        if(SimpleDbSchemaAttrs.Option.json.isEqual(type)){
          //  System.out.println("adding fk : "+getName()+"."+column + "->"+parent.getName());
            parent.addColumn(getName(), DataObject.ObjectType.String);
            addColumn(column, DataObject.ObjectType.String);
            Map<String,String> keyColumns = new HashMap<String,String>();
            keyColumns.put(column, getName());
            SchemaForeignKey fk = new SchemaForeignKey(name, this.name, keyColumns);
            addForeignKey(fk);
        }else{
          //  System.out.println(getName()+ " is domain");
          //  System.out.println(this);
            SchemaKey parentKey = parent.getPrimaryKey();
            if(parentKey == null)
                throw new IllegalStateException("no key defined for " + parent.getName());
            String parentColumn = parentKey.getColumns().get(0);
            Map<String,String> fkMap = new HashMap<String,String>();
            fkMap.put(column, parentColumn);
            SchemaForeignKey fk = new SchemaForeignKey(column, parentColumn, fkMap);
            addForeignKey(fk);
        }
    }

    @Override
    public Map<SchemaAttr,String> getDefaultAttrs(){
        Map sAttr = super.getDefaultAttrs();
        Map<SchemaAttr, String> dAttr = new HashMap<SchemaAttr, String>();
        for(SimpleDbSchemaAttrs attr: SimpleDbSchemaAttrs.values()){
            if(attr.getDefaultOption()!=null)
                dAttr.put(attr, attr.getDefaultOption().toString());
        }
        sAttr.putAll(dAttr);
        return sAttr;
    }
}