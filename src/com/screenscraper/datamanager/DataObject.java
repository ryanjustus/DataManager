/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A DataObject stores each individual piece of data in a DataNode along with
 * information of its type to facilitate writing it out to a database.
 * @author ryan
 */
public class DataObject
{
    private static Log log = LogFactory.getLog(DataObject.class);    

    public interface ValueParser<T>{
        T parseFromString(String input);
        T parseFromNumber(Number n);
    }

    public enum ObjectType{

        Object("java.lang.Object", false, new ValueParser<Object>(){

            public Object parseFromString(String input) {
                return input;
            }

            public Object parseFromNumber(Number n) {
                return n;
            }
        }),
        InputStream("java.io.InputStream", false, new ValueParser<InputStream>(){

            public InputStream parseFromString(String input) {
                throw new UnsupportedOperationException("Parsing a String into a Stream is unsupported");
            }

            public InputStream parseFromNumber(Number n) {
                throw new UnsupportedOperationException("Parsing a Number into a Stream is unsupported.");
            }
        }),
        Byte("java.lang.Byte", false, new ValueParser<Byte>(){

            public Byte parseFromString(String input) {
                return java.lang.Byte.parseByte(input);
            }

            public Byte parseFromNumber(Number n) {
                return n.byteValue();
            }

        }),
        String("java.lang.String", false, new ValueParser<String>(){

            public String parseFromString(String input) {
                return input;
            }

            public String parseFromNumber(Number n) {
                return n.toString();
            }
        }),
        ByteArray("[B", false,new ValueParser<byte[]>(){

            public byte[] parseFromString(String input) {
                return input.getBytes();
            }

            public byte[] parseFromNumber(Number n) {
               return parseFromString(n.toString());
            }
        }),
        Integer("java.lang.Integer", true,new ValueParser<Integer>(){
            /**
             * This isn't the fastest but I tried to make it robust
             * for a variety of String input
             */
            public Integer parseFromString(String input) {
                return parseFromNumber(extractNumberFromString(input));
            }

            public Integer parseFromNumber(Number n) {
                //return integer part of number
                return n.intValue();
            }
        }),
        Short("java.lang.Short", true,new ValueParser<Short>(){

            public Short parseFromString(String input) {
                return parseFromNumber(extractNumberFromString(input));
            }

            public Short parseFromNumber(Number n) {
                return n.shortValue();
            }
        }),
        Float("java.lang.Float", true,new ValueParser<Float>(){

            public Float parseFromString(String input) {
                return parseFromNumber(extractNumberFromString(input));
            }

            public Float parseFromNumber(Number n) {
                return n.floatValue();
            }
        }),
        Double("java.lang.Double", true,new ValueParser<Double>(){

            public Double parseFromString(String input) {
                return parseFromNumber(extractNumberFromString(input));
            }

            public Double parseFromNumber(Number n) {
                return n.doubleValue();
            }
        }),
        Long("java.lang.Long", true, new ValueParser<Long>(){

            public Long parseFromString(String input) {
                return parseFromNumber(extractNumberFromString(input));
            }

            public Long parseFromNumber(Number n) {
                return n.longValue();
            }
        }),
        BigDecimal("java.math.BigDecimal", true,new ValueParser<BigDecimal>(){

            public BigDecimal parseFromString(String input) {
                return parseFromNumber(extractNumberFromString(input));
            }

            public BigDecimal parseFromNumber(Number number) {
                if(number instanceof java.math.BigDecimal)
                    return (java.math.BigDecimal)number;
                else if(number instanceof java.math.BigInteger)
                    return new java.math.BigDecimal((java.math.BigInteger)number);
                else if(number instanceof java.lang.Long)
                    return new java.math.BigDecimal((java.lang.Long)number);
                else if(number instanceof java.lang.Integer)
                    return new java.math.BigDecimal((java.lang.Integer)number);
                else if(number instanceof java.lang.Double)
                    return new java.math.BigDecimal((java.lang.Double)number);
                else if(number instanceof java.lang.Float)
                    return new java.math.BigDecimal((java.lang.Float)number);
                else
                    return new java.math.BigDecimal(number.toString());
            }
        }),
        Boolean("java.lang.Boolean", false, new ValueParser<Boolean>(){

            public Boolean parseFromString(String input) {
                return java.lang.Boolean.valueOf(input);
            }

            public Boolean parseFromNumber(Number n) {
                if(n.equals(0))
                    return java.lang.Boolean.FALSE;
                return java.lang.Boolean.TRUE;
            }
        }),
        Date("java.sql.Date", false, new ValueParser<java.sql.Date>(){

            public java.sql.Date parseFromString(String input) {
                try{
                    return new java.sql.Date(java.lang.Long.parseLong(input));
                }catch(NumberFormatException e){
                    return null;
                }
            }

            public java.sql.Date parseFromNumber(Number n) {
                return new java.sql.Date(n.longValue());
            }
        }),
        Time("java.sql.Time", false, new ValueParser<java.sql.Time>(){
             public java.sql.Time parseFromString(String input) {
                try{
                    return new java.sql.Time(java.lang.Long.parseLong(input));
                }catch(NumberFormatException e){
                    return null;
                }
            }

            public java.sql.Time parseFromNumber(Number n) {
                return new java.sql.Time(n.longValue());
            }
        }),
        Timestamp("java.sql.Timestamp", true, new ValueParser<java.sql.Timestamp>(){

            public java.sql.Timestamp parseFromString(String input) {
                try{
                    Timestamp t = new Timestamp(java.lang.Long.parseLong(input));
                    return t;
                }catch(NumberFormatException e){
                    return null;
                }
            }

            public Timestamp parseFromNumber(Number n) {
                return new Timestamp(n.longValue());
            }
        });

        String javaName;
        Class javaClass;
        boolean numeric;
        Enum value;
        ValueParser parser;

        ObjectType(String name, boolean numeric, ValueParser parser){
            this.javaName = name;
            this.numeric=numeric;
            this.parser = parser;
            try{
                javaClass = Class.forName(name);                
            }catch(ClassNotFoundException e){
                log.warn("Class type not supported, setting to java.lang.Object");      
                javaClass=java.lang.Object.class;
            }
        }

        public Class getObjectClass(){
            return javaClass;
        }

        public String getObjectClassName(){
            return javaName;
        }

        public boolean isNumeric(){
            return numeric;
        }

        ValueParser getParser(){
          return parser;
        }

        public static ObjectType fromObject(Object o){
            if(o instanceof String)
                return ObjectType.String;
            if(o instanceof byte[])
                return  ObjectType.ByteArray;
            else if (o instanceof Short)
                return ObjectType.Short;
            else if(o instanceof Integer)
                return ObjectType.Integer;
            else if(o instanceof Byte)
                return ObjectType.Byte;
            else if(o instanceof Float)
                return ObjectType.Float;
            else if(o instanceof Double)
                return ObjectType.Double;
            else if(o instanceof Long)
                return ObjectType.Long;
            else if(o instanceof BigDecimal)
                return ObjectType.BigDecimal;
            else if(o instanceof Boolean)
                return ObjectType.Boolean;
            else if(o instanceof java.sql.Date)
                return ObjectType.Date;
            else if(o instanceof java.sql.Time)
                return ObjectType.Time;
            else if(o instanceof java.sql.Timestamp)
                return ObjectType.Timestamp;
            else if(o instanceof java.io.InputStream)
                return ObjectType.InputStream;
            else
                return ObjectType.Object;
        }

        public boolean verifyClassType(Object o){
            Class c = getObjectClass();
            if(c.isInstance(o)){
                return true;
            }
            return false;
        }


        public static ObjectType fromString(String className){
            if(className==null){
                log.warn("null className, setting to ObjectType.Object");
                return ObjectType.Object;
            }
            try{
                for(ObjectType e : ObjectType.values()){
                    if(e.toString().equalsIgnoreCase(className))
                        return e;
                }
                return ObjectType.valueOf(className);
            }catch(IllegalArgumentException e){
                log.warn("unsupported className, "+className+", setting to ObjectType.Object");
                return ObjectType.Object;
            }
        }

        @Override
        public String toString(){
            return javaName;
        }
    }

    private Object value;
    private ObjectType type;
    
    /**
     * Get a DataObject with a custom parser
     * @param value
     * @param type
     * @param parser 
     */
    public DataObject(Object value, ObjectType type, ValueParser parser){
        this.type=type;
        if(type==null){
            type = DataObject.ObjectType.String;
        }
        if(value==null){
            this.value=null;
        }else if(value.getClass()==type.getObjectClass()){
            this.value=value;
        }else if(value instanceof String){
            this.value=parser.parseFromString((String)value);
        }else if(value instanceof Number){
            this.value=parser.parseFromNumber((Number)value);
        }else{    
            throw new IllegalArgumentException("Object type " + value.getClass() + " cannot be used for ObjectType" + type.toString());
        }
    }

    /**
     * Get a DataObject with the default parser
     * @param value
     * @param type 
     */
    public DataObject(Object value, ObjectType type)
    {
        this.type=type;
        if(type==null){
            type = DataObject.ObjectType.String;
        }
        if(value==null){
            this.value=null;
        }else if(value.getClass()==type.getObjectClass()){
            this.value=value;
        }else if(value instanceof String){
            this.value=type.parser.parseFromString((String)value);
        }else if(value instanceof Number){
            this.value=type.parser.parseFromNumber((Number)value);
        }else{    
            throw new IllegalArgumentException("Object type " + value.getClass() + " cannot be used for ObjectType" + type.toString());
        }
    }


	/**
	 * gets the ObjectType of the DataObject.  ObjectType has information that helps the DataManager convert from
	 * Java types to database target types
	 * @return ObjectType
	 */
    public ObjectType getType(){
        return type;
    }

	/**
	 * Return the object that is contained in the DataObject
	 * @return
	 */
    public Object getObject(){
        return value;
    }

	/**
	 * Attempt to serialize the given object
	 * @param o
	 * @return
	 */
    public static byte[] serializeObject(Object o)
    {
        ByteArrayOutputStream bStream = new ByteArrayOutputStream();
        ObjectOutputStream oStream;
        try {
            oStream = new ObjectOutputStream(bStream);
            oStream.writeObject (o);
            byte[] byteVal = bStream. toByteArray();
            return byteVal;
        }
        catch (IOException ex) {
            log.error("Error while serializing Object, unable to save with DataManager: " + o, ex);
            return null;
        }
    }

	/**
	 * An object is equal if it is an instance of DataObject, the ObjectType is equivalent, and the equals method on the
	 * contained object returns true.
	 * @param o
	 * @return
	 */
    @Override
    public boolean equals(Object o){
        if(o instanceof DataObject){
            DataObject d = (DataObject)o;
            if(d.getType()==getType() && d.getObject().equals(getObject()))
                return true;
        }else{
            return false;
        }
        return false;
    }

	/**
	 * The hashcode is calculated based on the ObjectType and contained object
	 * @return
	 */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.value != null ? this.value.hashCode() : 0);
        hash = 29 * hash + (this.type != null ? this.type.hashCode() : 0);
        return hash;
    }

	/**
	 * Returns a string representation of the Object
	 * @return
	 */
    @Override
    public String toString()
    {
        if(value==null)
            return null;
        if(type.equals(ObjectType.Object))
        {
            try
            {
                //attempt to serialize object
                byte[] b = serializeObject(value);
                if(b==null)
                    return null;
                String s = new String(b, "UTF-16");
                return s;
            }
            catch(IOException e)
            {
                return String.valueOf(value);
            }
        }
        else
            return String.valueOf(value);
    }

	/**
	 * Retrieve the object as a BigDecimal or throws a ClassCastException if it can't
	 * @return
	 */
    public BigDecimal getBigDecimal(){
        if(this.type==DataObject.ObjectType.BigDecimal){
            return (BigDecimal)value;
        }else if(this.type==ObjectType.Integer){
            return new BigDecimal((Integer)value);
        }else if(this.type==ObjectType.Long){
            return new BigDecimal((Long)value);
        }else if(this.type==ObjectType.Double){
            return new BigDecimal((Double)value);
        }else if(this.type==ObjectType.Float){
            return new BigDecimal((Float)value);
        }else if(this.type==ObjectType.Short){
            return new BigDecimal((Short)value);
        }else if(type==ObjectType.Byte){
            return new BigDecimal((Byte)value);
        }
        throw new ClassCastException("cannot convert " + type + " to BigDecimal for " + value);
    }

	/**
	 * Retrieve the object as a Boolean or throws a ClassCastException if it can't
	 * @return
	 */
    public Boolean getBoolean(){
        if(this.type==DataObject.ObjectType.Boolean){           
            return (Boolean)this.value;
        }
        throw new ClassCastException("cannot convert " + type + " to Boolean for " + value);
    }


	/**
	 * Retrieve the object as a Byte or throws a ClassCastException if it can't
	 * @return
	 */
    public Byte getByte(){
        if(this.type==DataObject.ObjectType.Byte){
            return (Byte)this.value;
        }
        throw new ClassCastException("cannot convert " + type + " to Byte for " + value);
    }

	/**
	 * Retrieve the object as a java.sql.Date or throws a ClassCastException if it can't
	 * @return
	 */
    public java.sql.Date getDate(){
        if(this.type==ObjectType.Date){
            return (java.sql.Date)this.value;
        }
        throw new ClassCastException("cannot convert " + type + " to java.sql.Date for " + value);
    }

	/**
	 * Retrieve the object as a java.sql.Time or throws a ClassCastException if it can't
	 * @return
	 */
    public java.sql.Time getTime(){
        if(this.type==ObjectType.Time){
            return (java.sql.Time)this.value;
        }
        throw new ClassCastException("cannot convert " + type + " to java.sql.Time for " + value);
    }

	/**
	 * Retrieve the object as a java.sql.Timestamp or throws a ClassCastException if it can't
	 * @return
	 */
    public java.sql.Timestamp getTimestamp(){
        if(this.type==ObjectType.Timestamp){
            return (java.sql.Timestamp)this.value;
        }
        throw new ClassCastException("cannot convert " + type + " to java.sql.Timestamp for " + value);
    }

	/**
	 * Retrieve the object as a Double or throws a ClassCastException if it can't
	 * @return
	 */
     public Double getDouble(){
        if(this.type==ObjectType.Double){
            return (Double)this.value;
        }else if(type==ObjectType.Float){
            return new Double((Float)value);
        }else if(type==ObjectType.Long){
            return new Double((Long)value);
        }else if(type==ObjectType.Integer){
            return new Double((Integer)value);
        }else if(type==ObjectType.Short){
            return new Double((Short)value);
        }else if(type==ObjectType.Byte){
            return new Double((Byte)value);
        }
        throw new ClassCastException("cannot convert " + type + " to Double for " + value);
     }

	/**
	 * Retrieve the object as a Float or throws a ClassCastException if it can't
	 * @return
	 */
     public Float getFloat(){
        if(type==ObjectType.Float){
            return (Float)value;
        }else if(type==ObjectType.Long){
            return new Float((Long)value);
        }else if(type==ObjectType.Integer){
            return new Float((Integer)value);
        }else if(type==ObjectType.Short){
            return new Float((Short)value);
        }else if(type==ObjectType.Byte){
            return new Float((Byte)value);
        }
        throw new ClassCastException("cannot convert " + type + " to Float for " + value);
     }

	/**
	 * Retrieve the object as a Integer or throws a ClassCastException if it can't
	 * @return
	 */
     public Integer getInt(){
        if(type==ObjectType.Integer){
            return (Integer)value;
        }else if(type==ObjectType.Short){
            return new Integer((Short)value);
        }else if(type==ObjectType.Byte){
            return new Integer((Byte)value);
        }
        throw new ClassCastException("cannot convert " + type + " to Integer for " + value);
     }

	/**
	 * Retrieve the object as a Long or throws a ClassCastException if it can't
	 * @return
	 */
     public Long getLong(){
         if(type==ObjectType.Long){
            return (Long)value;
        }else if(type==ObjectType.Integer){
            return new Long((Integer)value);
        }else if(type==ObjectType.Short){
            return new Long((Short)value);
        }else if(type==ObjectType.Byte){
            return new Long((Byte)value);
        }
        throw new ClassCastException("cannot convert " + type + " to Long for " + value);
     }

	/**
	 * Retrieve the object as a Short or throws a ClassCastException if it can't
	 * @return
	 */
     public Short getShort(){
        if(type==ObjectType.Short){
            return (Short)value;
        }else if(type==ObjectType.Byte){
            return new Short((Byte)value);
        }
        throw new ClassCastException("cannot convert " + type + " to Short for " + value);
     }

	/**
	 * Retrieve the object as a Byte[] or throws a ClassCastException if it can't
	 * @return
	 */
    public Byte[] getBytes(){
        if(type==ObjectType.ByteArray){
            return (Byte[])value;
        }
        throw new ClassCastException("cannot convert " + type + " to Byte[] for " + value);
    }

	/**
	 * Retrieve the object as a String
	 * @return
	 */
    public String getString(){
        if(type==ObjectType.String){
            return (String)value;
        }
        return String.valueOf(value);
    }

	/**
	 * Retrieve the object as an InputStream.  If it is not already an InputStream type it will attempt to
	 * serialize the object via DataObject.serializeObject
	 * @return
	 */
    public InputStream getInputStream(){

        if(type==ObjectType.InputStream){
            return (InputStream)value;
        }
        return new ByteArrayInputStream(DataObject.serializeObject(value));
    }

	/**
	 * Attempts to extract a number from the String with the regex "-?[0-9]+(\\.[0-9]+)?"
	 * @param input
	 * @return
	 */
    private static BigDecimal extractNumberFromString(String input){
         //get rid of all the extra non number junk
        Pattern NUMBER_PATTERN = Pattern.compile("-?[0-9]+(\\.[0-9]+)?");
        Matcher m = NUMBER_PATTERN.matcher(input);
        if(m.find()){
            input=m.group();
        }else{
            input="";
        }
        //turn it into a number
        return new java.math.BigDecimal(input);
    }
}