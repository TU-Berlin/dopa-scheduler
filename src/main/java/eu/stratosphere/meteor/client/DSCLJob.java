package eu.stratosphere.meteor.client;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

import eu.stratosphere.meteor.client.ClientConnectionFactory;
import eu.stratosphere.meteor.client.DOPAClient;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.common.JobStateListener;
import eu.stratosphere.meteor.common.MessageBuilder;
import eu.stratosphere.meteor.common.ResultFileHandler;

/**
 * Implements the comparable interface to define the differences between jobs.
 * Two DSCLJobs are the same if they JobIDs are same.
 *
 * @author André Greiner-Petter
 * 
 */
public class DSCLJob {
	
	/**
	 * Unique job ID to identify this job
	 */
	private final String JOB_ID;
	
	/**
	 * Unique client ID
	 */
	private final String CLIENT_ID;
	
	/**
	 * To handle traffic between a job object itself and the scheduler service
	 */
	private final ClientConnectionFactory connectionFac;
	
	/**
	 * The current job status.
	 */
	private JobState currState;
	
	/**
	 * The script of this job.
	 */
	private String meteorScript;
	
	/**
	 * List of all listeners at this job
	 */
	private LinkedList<JobStateListener> listeners;
	
	/**
	 * Map of all result file handlers. Key is the value of file index.
	 * This key is unique for each job.
	 */
	private HashMap<Integer, ResultFileHandler> resultHandler;
	
	/**
	 * This map contains the result file for its specified index. Normally null.
	 */
	private HashMap<Integer, File> results;
	
	/**
	 * Map of internal links. Filled after send a requests
	 */
	private HashMap<Integer, String> linksOfResults;
	
	/**
	 * Create a new DSCLJob object. It needs to get the connection factory to register JobStateListeners.
	 * 
	 * @param connectionFac to register state listeners
	 * @param jobID to identify this job
	 * @param meteorScript the script of this job
	 */
	protected DSCLJob( final ClientConnectionFactory connectionFac, final String clientID, final String jobID, String meteorScript ){
		this.connectionFac = connectionFac;
		this.CLIENT_ID = clientID;
		this.JOB_ID = jobID;
		this.meteorScript = meteorScript;
		
		// initialize job status
		this.currState = JobState.INITIALIZE;
		
		// declare collections
		this.listeners = new LinkedList<JobStateListener>();
		this.linksOfResults = new HashMap<Integer, String>();
		this.resultHandler = new HashMap<Integer, ResultFileHandler>();
		this.results = new HashMap<Integer, File>();
	}
	
	/**
	 * Sets the new status of this job. It is protected so only the DOPAClient and the ClientConnectionFactory
	 * can call this method to change status.
	 * 
	 * @param newState one of the enum JobState
	 */
	protected void setStatus( JobState newState ){
		this.currState = newState;
	}
	
	/**
	 * Sets meteor script (not visible for normal users)
	 * @param meteorScript
	 */
	protected void setMeteorScript( String meteorScript ){
		this.meteorScript = meteorScript;
	}
	
	/**
	 * Deletes the handler for result file.
	 * @param fileIdx specify result
	 */
	protected void deleteStatusHandler( int fileIdx ){
		this.resultHandler.remove( fileIdx );
	}
	
	/**
	 * Sets the output path.
	 * @param index of result link in job
	 * @param path itself
	 */
	protected void setResultLink( int index, String path ){
		this.linksOfResults.put( index, path );
	}
	
	/**
	 * It adds the result file specified by file index to internal list of all results.
	 * @param fileIdx of result
	 * @param file result
	 */
	protected void setResultFile( int fileIdx, File file ){
		this.results.put( fileIdx, file );
	}
	
	/**
	 * Returns the map of all results saved on this job.
	 * @return a modifiable map of results
	 */
	public HashMap<Integer, File> getResults() {
		return results;
	}
	
	/**
	 * Returns the current status of this job.
	 * @return current job status
	 */
	public JobState getStatus(){
		return currState;
	}
	
	/**
	 * Returns the ID of this object.
	 * @return ID
	 */
	public String getID(){
		return JOB_ID;
	}
	
	/**
	 * @return meteor script
	 */
	public String getMeteorScript(){
		return meteorScript;
	}
	
	/**
	 * Return the link of a specified result.
	 * @return link
	 */
	public String getResultLink( int index ){
		return linksOfResults.get( index );
	}
	
	/**
	 * Returns an unmodifiable list of all result handlers currently executing on this job.
	 * @return map with key for file index and the specified handler
	 */
	public Map<Integer, ResultFileHandler> getResultHandler(){
		return Collections.unmodifiableMap(resultHandler);
	}
	
	/**
	 * Returns an unmodifiable list of all listeners of this job.
	 * @return list of all listeners
	 */
	public List<JobStateListener> getListeners(){
		return Collections.unmodifiableList( listeners );
	}
	
	/**
	 * Adds a new listener.
	 * @param listener
	 * @return true if the collection changed
	 */
	public boolean addJobStateListener( JobStateListener listener ){
		return this.listeners.add(listener);
	}
	
	/**
	 * Try to remove the specified listener.
	 * @param listener
	 * @return true if the collection changed
	 */
	public boolean removeJobStateListener( JobStateListener listener ){
		return this.listeners.remove(listener);
	}
	
	/**
	 * Sends a request to get specified blocks of result file by specified index.
	 * @param fileIndex of result file
	 * @param desiredBlockSize size of block you want for one block, scheduler can choose own sizes if necessary
	 * @param maxNumberOfBlocks threshold for blocks
	 * @param handler to handle each incoming block and put them all together
	 */
	public void requestResult( int fileIndex, int desiredBlockSize, long maxNumberOfBlocks, ResultFileHandler handler ){
		try {
			// build message
			JSONObject request = MessageBuilder.buildRequestResult(CLIENT_ID, JOB_ID, fileIndex, desiredBlockSize, maxNumberOfBlocks);
			
			// add given handler to internal list
			this.resultHandler.put( fileIndex, handler );
			
			// create result consumer to refresh states and invoke handlers automatically
			String corrID = DOPAClient.getRandomID();
			ResultConsumer consumer = this.connectionFac.getResultConsumer(corrID);
			
			// finally send the request
			this.connectionFac.sendRequest(consumer, request, corrID);
		} catch (ShutdownSignalException | ConsumerCancelledException
				| IOException | InterruptedException e) {
			DOPAClient.LOG.error("Cannot send result request to scheduler.", e);
		}
	}
	
	/**
	 * Get HDFS path of output file for use in follow-up jobs. You specified the link by a given
	 * index. The connection factory add the link automatically after received.
	 * @param fileIndex specified index
	 */
	public void getLink( int fileIndex ){
		try { // build request object and send message
			JSONObject request = MessageBuilder.buildGetLink(CLIENT_ID, JOB_ID, fileIndex);
			
			String corrID = DOPAClient.getRandomID();
			LinkConsumer consumer = this.connectionFac.getLinkConsumer(corrID);
			this.connectionFac.sendRequest(consumer, request, corrID );
		} catch (ShutdownSignalException | ConsumerCancelledException
				| IOException | InterruptedException e) {
			DOPAClient.LOG.error( "Cannot send request to get output path.", e ); 
		}
	}
	
	/**
	 * Its abort this specified job on the scheduler site. This job is deleted on scheduler site when the current 
	 * status changed to {@code JobState.DELETED}.
	 */
	public void abortJob(){
		try { // build request object and send message
			JSONObject request = MessageBuilder.buildJobAbort(CLIENT_ID, JOB_ID);
			this.connectionFac.sendRequest(null, request, DOPAClient.getRandomID() );
		} catch (ShutdownSignalException | ConsumerCancelledException
				| IOException | InterruptedException e) {
			DOPAClient.LOG.error( "Cannot send request to abort a job.", e ); 
		}
	}
}