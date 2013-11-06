package eu.stratosphere.meteor.server.executor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import eu.stratosphere.meteor.server.executor.RRJob;

/**
 * A iterator to get next elements in round robin algorithm for clients and their jobs.
 * It although includes the complete list of jobs waiting to execute by the scheduler.
 *
 * @author André Greiner-Petter
 * 
 */
public class RoundRobin implements Iterator<RRJob> {
	/**
	 * A list of the client IDs to iterate through all clients in round robin algorithm.
	 * Handled like a queue, FIFO principle.
	 */
	private LinkedList<String> clientRRList;
	
	/**
	 * Mapped a complete job list to each client (client ID). The inner list contains the
	 * job objects itself. So it's mapping <clientID> -> List of <RRJobs>
	 */
	private HashMap< String, LinkedList<RRJob> > mappedRRJobList;
	
	/**
	 * A collection of all jobs and clients included in that map.
	 */
	private HashMap< String, HashMap<String, RRJob> > collectionClientsAndJobs;
	
	/**
	 * Create iterative object
	 */
	public RoundRobin(){
		this.clientRRList = new LinkedList<String>();
		this.mappedRRJobList = new HashMap< String, LinkedList<RRJob> >();
		this.collectionClientsAndJobs = new HashMap< String, HashMap<String, RRJob> >();
	}
	
	/**
	 * Has next if there are clients in the list.
	 */
	@Override
	public boolean hasNext() {
		return !mappedRRJobList.isEmpty();
	}
	
	/**
	 * Round Robin algorithm with a specified list of keys and a list of elements for each key.
	 * A key contains many elements. A round robin cycle take the next client and only the next
	 * job element of the client. This job element are returns and deleted from list. The client
	 * put back to the last position of client list.
	 * @return next element or null if no more elements included
	 * @throws NoSuchElementException if the list is empty
	 */
	@Override
	public RRJob next() throws NoSuchElementException {
		if ( clientRRList.isEmpty() ) return null;
		
		// get next element
		String topKey = clientRRList.removeFirst();
		LinkedList<RRJob> elementList = mappedRRJobList.get(topKey);
		
		// if no more elements mapped on that key, delete the key
		if ( elementList == null ) {
			mappedRRJobList.remove(topKey);
			collectionClientsAndJobs.remove(topKey);
			// the client is just deleted from the client list
			return next();
		}
		
		// else get next element and put topKey to last position
		RRJob topElement = elementList.pop();
		
		// put the client back to working list
		clientRRList.addLast( topKey );
		
		// returns top element
		return topElement;
	}
	
	/**
	 * Nothing to do here. Only specified removes allowed.
	 * @deprecated this method do nothing. Use the specified remove methods with given informations
	 * 		to remove an element
	 */
	@Override
	@Deprecated
	public void remove() {}
	
	/**
	 * Removes the specified key with all mapped lists.
	 * @param key
	 */
	public void remove( String key ){
		mappedRRJobList.remove(key);
		clientRRList.remove(key);
		collectionClientsAndJobs.remove(key);
	}
	
	/**
	 * Add a new key
	 * @param key
	 */
	public void add( String key ){
		LinkedList<RRJob> tmp = mappedRRJobList.get(key);
		if ( tmp != null ) return; // already included
		mappedRRJobList.put(key, new LinkedList<RRJob>());
		clientRRList.add(key);
		collectionClientsAndJobs.put(key, new HashMap<String, RRJob>());
	}
	
	/**
	 * Removes a specified element.
	 * @param key
	 * @param element
	 * @return true if the collection changed
	 */
	public boolean remove( String key, RRJob element ){
		// get job list of key
		LinkedList<RRJob> jobList = this.mappedRRJobList.get(key);
		
		// if no elements mapped on this key, return false (nothing changed)
		if ( jobList == null ) return false;
		
		// remove it from all registered jobs/clients
		collectionClientsAndJobs.get(key).remove(element);
		
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
	public boolean add( String key, RRJob element ){
		// get current list of given key
		LinkedList<RRJob> jobList = this.mappedRRJobList.get(key);
		
		// if the list doesn't exists the key doesn't exists
		if ( jobList == null || jobList.isEmpty() ) {
			// so add key and new list
			clientRRList.add( key );
			jobList = new LinkedList<RRJob>();
			this.mappedRRJobList.put(key, jobList);
			this.collectionClientsAndJobs.put(key, new HashMap<String, RRJob>());
		}
		
		// add the element to collection
		this.collectionClientsAndJobs.get(key).put(element.getJobID(), element);
		
		// at least add the element to the job list
		return jobList.add( element );
	}
	
	/**
	 * Removes the specified job by given IDs. It returns true if that changed the inner lists
	 * or false if nothing happened (means the job doesn't existed).
	 * @param clientID of client
	 * @param jobID of job
	 * @return true if the lists changes or false if nothing changed
	 */
	public boolean remove( String clientID, String jobID ){
		RRJob tmp = this.collectionClientsAndJobs.get(clientID).get(jobID);
		if ( tmp != null ) {
			return this.remove(clientID, tmp);
		}
		else return false;
	}
	
	/**
	 * Returns the job object specified by given client ID and job ID
	 * @param clientID the client submitted that job
	 * @param jobID job ID
	 * @return job object
	 */
	public RRJob get( String clientID, String jobID ){
		return this.collectionClientsAndJobs.get(clientID).get(jobID);
	}
	
	/**
	 * Returns true if the round robin algorithm contains a specified job or false if not.
	 * @param clientID
	 * @param jobID
	 * @return true if the job exists in that algorithm or false if not
	 */
	public boolean contains( String clientID, String jobID ){
		if ( !collectionClientsAndJobs.containsKey(clientID) ) return false;
		else if ( !collectionClientsAndJobs.get(clientID).containsKey(jobID) ) return false;
		else return true;
	}
	
	/**
	 * Hard reset of inner architecture. Resets all clients and all jobs.
	 */
	public void hardReset(){
		clientRRList = new LinkedList<String>();
		mappedRRJobList = new HashMap<String, LinkedList<RRJob>>();
		collectionClientsAndJobs = new HashMap<String, HashMap<String, RRJob>>();
	}
	
	@Override
	public String toString(){
		String out = "List of clients in RR: " + clientRRList + System.lineSeparator()
				+ "List of Jobs per Client: ";
		
		for ( String client : mappedRRJobList.keySet() )
			out += client + ": " + mappedRRJobList.get(client).toString() + ";";
		
		return out;
	}
}
