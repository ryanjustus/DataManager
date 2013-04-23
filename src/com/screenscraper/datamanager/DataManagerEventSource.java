/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class that passes the events around.  The Schema objects contains a reference
 * to this and forwards the methods.
 * @author ryan
 */
public class DataManagerEventSource {

	public enum EventFireTime{

		/**
		 * called whenever the DataManager creates a new DataNode (DataManager.getNewDataNode)
		 */
		onCreate,


		/**
		 * called immediately before the DataNode is written (DataWriter.write),
		 * after all internal data transformations, additions
		 */
		onWrite,


		/**
		 * Called when values are to be updated in the underlying datasource as opposed
		 * to written as new values
		 */
		onUpdate,


		/**
		 * called immediately after the DataNode is written (DataWriter.write),
		 * at this point any values that the write has added are present
		 */
		afterWrite,

		/**
		 * called when DataManager.commit is called, after nodes are linked together
		 */
		onCommit,

		/**
		 * called whenever DataManager.addData is called
		 */
		onAddData,
		/**
		 * Called immediately before a write is performed
		 */
		onInsert,

		/**
		 * Called when the datamanager receives an exception on a write
		 */
		onWriteError;
	}

	private Map<EventFireTime, List<DataManagerEventListener>> listeners;

	/**
	 *
	 */
	public DataManagerEventSource(){
		listeners = new EnumMap<EventFireTime, List<DataManagerEventListener>>(EventFireTime.class);
		//initialize the map with empty Lists for each valid event fire time
		for(EventFireTime e: EventFireTime.values()){
			listeners.put(e, new ArrayList<DataManagerEventListener>());
		}
	}

	/**
	 * Add a listener
	 * @param when
	 * @param listener
	 * @return
	 */
	public boolean addEventListener(EventFireTime when, DataManagerEventListener listener){
		return listeners.get(when).add(listener);
	}

	/**
	 * remove a listener
	 * @param when
	 * @param listener
	 * @return
	 */
	public boolean removeEventListener(EventFireTime when, DataManagerEventListener listener){
		return listeners.get(when).remove(listener);
	}

	/**
	 * retrieve all event listeners
	 * @param when
	 * @return
	 */
	public List<DataManagerEventListener> getEventListeners(EventFireTime when){
		return listeners.get(when);
	}

	/**
	 * Fire event
	 * @param when
	 * @param evt
	 */
	public void fireEvent(EventFireTime when, DataManagerEvent evt){
		List<DataManagerEventListener> eventListeners = listeners.get(when);
		for(DataManagerEventListener listener: eventListeners){
			listener.handleEvent(evt);
		}
	}
}