package eu.stratosphere.meteor.client;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.meteor.common.JobStateListener;
import eu.stratosphere.meteor.common.JobStates;

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
	 * The current job status.
	 */
	private JobStates currState;
	
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
	protected DSCLJob( final String ID, String meteorScript ){
		this.JOB_ID = ID;
		this.meteorScript = meteorScript;
		this.currState = JobStates.INITIALIZE;
		this.listeners = new LinkedList<JobStateListener>();
	}
	
	/**
	 * Sets the new status of this job. It is protected so only the DOPAClient and the ClientConnectionFactory
	 * can call this method to change status.
	 * 
	 * @param newState one of the enum JobStates
	 */
	protected void setStatus( JobStates newState ){
		this.currState = newState;
	}
	
	/**
	 * Returns the current status of this job
	 * @return 
	 */
	public JobStates getStatus(){
		return currState;
	}
	
	/**
	 * Returns the ID
	 * @return ID
	 */
	public String getID(){
		return JOB_ID;
	}
	
	public void getResult(){
		// TODO return "file"
	}
	
	public String getLink( String fileID ){
		// TODO return link from file
		return null;
	}
	
	/**
	 * Returns an unmodifiable list of all listeners of this job.
	 * @return
	 */
	protected List<JobStateListener> getListeners(){
		return Collections.unmodifiableList( listeners );
	}
	
	public boolean addJobStateListener( JobStateListener listener ){
		return this.listeners.add(listener);
	}
	
	public boolean removeJobStateListener( JobStateListener listener ){
		return this.listeners.remove(listener);
	}
	
	public void fetchResult( /* ProgessListener */ String fileID ){
		// TODO 
	}
	
	public void abort( String clientID, String jobID ){
		// TODO
	}
	
	public void resumeJob( String clientID, String jobID ){
		// TODO
	}
}
