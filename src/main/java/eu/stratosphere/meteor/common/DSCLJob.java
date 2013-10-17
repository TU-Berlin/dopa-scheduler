package eu.stratosphere.meteor.common;

import eu.stratosphere.meteor.client.ClientConnectionFactory;

/**
 * Implements the comparable interface to define the differences between jobs.
 * Two DSCLJobs are the same if they JobIDs are same.
 *
 * @author André Greiner-Petter
 * 
 */
public class DSCLJob implements Comparable<DSCLJob>{
	
	/**
	 * Global enumeration to identify the current job status
	 */
	public static enum State{
		// job states
		INITIALIZE("Initialize blank job object."), 
		SUBMIT("Job submitted to scheduler."), 
		WAITING("Job sitting on the waiting queue of the scheduler. Doesn't executed yet."), 
		RUNNING("Job running."),
		FINISHED("Job finished.");
		
		// string information for each state
		private String descr;
		
		/**
		 * Private constructor to set the descriptions
		 * @param descr
		 */
		private State( String descr ){
			this.descr = descr;
		}
		
		/**
		 * Override existing toString() method to return the description
		 * of the status and not the name itself.
		 */
		@Override
		public String toString(){
			return descr;
		}
	}
	
	private final String JOB_ID;
	
	private final ClientConnectionFactory connectionFac;
	private State currState;
	
	public DSCLJob( ClientConnectionFactory connectionFac, final String ID, String meteorScript ){
		this.connectionFac = connectionFac;
		this.JOB_ID = ID;
	}
	
	public String getID(){
		return JOB_ID;
	}
	
	public State getStatus(){
		return currState;
	}
	
	public void setStatus( State newState ){
		this.currState = newState;
	}
	
	public void getResult(){
		// TODO return "file"
	}
	
	public void fetchResult( /* ProgessListener */ String fileID ){
		// TODO 
	}
	
	public String getLink( String fileID ){
		// TODO return link from file
		return null;
	}
	
	public void abort( String clientID, String jobID ){
		// TODO
	}
	
	public void resumeJob( String clientID, String jobID ){
		// TODO
	}

	@Override
	public int compareTo(DSCLJob o) {
		if ( JOB_ID.matches(o.getID()) ) return 0;
		return 1;
	}
}
