/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.screenscraper.datamanager.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;

/**
 *
 * @author ryan
 */
public class LogAppender extends AppenderSkeleton implements Appender {


    private final List<Logger> loggers;
    public LogAppender(){
        loggers = new ArrayList<Logger>();     
    }
    
    public void addLogger(Logger logger){
        loggers.add(logger);
    }
    
    @Override
    protected void append(LoggingEvent le) {
        Priority level= le.level;
        String message = le.getRenderedMessage();
        try{
            if(Priority.DEBUG.isGreaterOrEqual(level)){
                for(Logger l: loggers){
                    l.debug(message);
                }
            }else if(Priority.INFO.isGreaterOrEqual(level)){
                for(Logger l: loggers){
                    l.info(message);
                }
            }else if(Priority.WARN.isGreaterOrEqual(level)){
                for(Logger l: loggers){
                    l.warn(message);
                }
            }else if(Priority.ERROR.isGreaterOrEqual(level)){
                Throwable t = le.getThrowableInformation().getThrowable();
                for(Logger l: loggers){
                    if(t==null){
                        l.error(message);
                    }else{
                        l.error(message, t);
                    }
                }                
            }else if(Priority.FATAL.isGreaterOrEqual(level)){
                System.err.println(message);
                le.getThrowableInformation().getThrowable().printStackTrace(System.err);
            }
        }catch(Exception e){
            e.printStackTrace(System.err);
        }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    public void close() {}
    
}
