package eu.stratosphere.meteor.client;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.meteor.client.job.ResultFileHandler;
import eu.stratosphere.meteor.client.listener.JobStateListener;
import eu.stratosphere.meteor.common.JobState;

/**
 * Implements the comparable interface to define the differences between jobs.
 * Two DSCLJobs are the same if they JobIDs are same.
 *
 * @author André Greiner-Petter
 * 
 */
public class DSCLJob{
	
	/**
	 * Unique job ID to identify this job
	 */
	private final String JOB_ID;
	
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
	 * Create a new DSCLJob object. It needs to get the connection factory to register JobStateListeners.
	 * 
	 * @param connectionFac to register state listeners
	 * @param ID to identify this job
	 * @param meteorScript the script of this job
	 */
	protected DSCLJob( final ClientConnectionFactory connectionFac, final String ID, String meteorScript ){
		this.connectionFac = connectionFac;
		this.JOB_ID = ID;
		this.meteorScript = meteorScript;
		this.currState = JobState.INITIALIZE;
		this.listeners = new LinkedList<JobStateListener>();
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
	 * 
	 * @param meteorScript
	 */
	protected void setMeteorScript( String meteorScript ){
		this.meteorScript = meteorScript;
	}
	
	/**
	 * Returns the current status of this job
	 * @return 
	 */
	public JobState getStatus(){
		return currState;
	}
	
	/**
	 * Returns the ID.
	 * @return ID
	 */
	public String getID(){
		return JOB_ID;
	}
	
	/**
	 * Returns an unmodifiable list of all listeners of this job.
	 * @return
	 */
	protected List<JobStateListener> getListeners(){
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
	 * 
	 * @param fileIndex
	 * @param desiredBlockSize
	 * @param maxNumberOfBlocks
	 * @param handler
	 */
	public void requestResult( int fileIndex, long desiredBlockSize, long maxNumberOfBlocks, ResultFileHandler handler ){
		/*
		 * TODO
		 * create ResultFileBlock with specified informations
		 */
	}
	
	/**
	 * Get hdfs path of output file for use in follow-up jobs.
	 * @param fileID
	 * @return
	 */
	public String getLink( int fileIndex ){
		// TODO return link from file
		return null;
	}
	
	public void abort( String clientID, String jobID ){
		// TODO
	}
	
	public void resumeJob( String clientID, String jobID ){
		// TODO
	}
}
