package eu.stratosphere.meteor.server.executor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A iterator to get next elements in round robin algorithm for clients and their jobs.
 * TODO include methods to handle all working lists, identify job by client and so on
 *
 * @author André Greiner-Petter
 *
 * @param <T> should be String (for ClientID)
 * @param <V> should be RRJob
 */
public class RoundRobin<T, V> implements Iterator<V> {
	/**
	 * List of clients to iterate through all clients
	 */
	private LinkedList<T> clientList;
	
	/**
	 * Mapped a client to its job list to iterate through its job.
	 */
	private HashMap< T, LinkedList<V> > clientJobs;
	
	/**
	 * Create iterative object
	 */
	public RoundRobin(){
		this.clientList = new LinkedList<T>();
		this.clientJobs = new HashMap< T, LinkedList<V> >();
	}
	
	/**
	 * Has next if there are clients in the list.
	 */
	@Override
	public boolean hasNext() {
		return !clientJobs.isEmpty();
	}
	
	/**
	 * Round Robin algorithm with a specified list of keys and a list of elements for each key.
	 * A key contains many elements. A round robin cycle take the next client and only the next
	 * job element of the client. This job element are returns and deleted from list. The client
	 * put back to the last position of client list.
	 * @return next element or null if no more elements included
	 */
	@Override
	public V next() {
		if ( clientList.isEmpty() ) return null;
		
		// get next element
		T topKey = clientList.pop();
		LinkedList<V> elementList = clientJobs.get(topKey);
		
		// if no more elements mapped on that key, delete the key
		if ( elementList == null ) {
			clientJobs.remove(topKey);
			return next();
		}
		
		// else get next element and put topKey to last position
		V topElement = elementList.pop();
		
		// put this element to end of lists
		elementList.addLast(topElement);
		
		// returns top element
		return topElement;
	}
	
	/**
	 * Nothing to do here. Only specified removes allowed.
	 */
	@Override
	public void remove() {}
	
	/**
	 * Removes the specified key with all mapped lists.
	 * @param key
	 */
	public void remove( T key ){
		clientJobs.remove(key);
		clientList.remove(key);
	}
	
	/**
	 * Add a new key
	 * @param key
	 */
	public void add( T key ){
		LinkedList<V> tmp = clientJobs.get(key);
		if ( tmp != null ) return;
		clientJobs.put(key, new LinkedList<V>());
	}
	
	/**
	 * Removes a specified element.
	 * @param key
	 * @param element
	 * @return true if the collection changed
	 */
	public boolean remove( T key, V element ){
		// get job list of key
		LinkedList<V> jobList = this.clientJobs.get(key);
		
		// if no elements mapped on this key, return false (nothing changed)
		if ( jobList == null ) return false;
		
		// return remove from list
		return jobList.remove( element );
	}
	
	/**
	 * Adds a new element of type <V> to the list of key <T>. Returns true
	 * if this list changed, false otherwise.
	 * @param key mapped to list of elements of type <V>
	 * @param element added to specified list of given key
	 * @return true if this list changed, false otherwise
	 */
	public boolean add( T key, V element ){
		// get current list of given key
		LinkedList<V> jobList = this.clientJobs.get(key);
		
		// if the list doesn't exists the key doesn't exists
		if ( jobList == null ) {
			// so add key and new list
			clientList.add( key );
			jobList = new LinkedList<V>();
			this.clientJobs.put(key, jobList);
		}
		
		// at least add the element
		return jobList.add( element );
	}
	
	/**
	 * Hard reset of inner architecture. Resets all clients and all jobs.
	 */
	public void hardReset(){
		clientList = new LinkedList<T>();
		clientJobs = new HashMap<T, LinkedList<V>>();
	}
}
