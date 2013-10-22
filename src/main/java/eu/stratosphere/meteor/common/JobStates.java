package eu.stratosphere.meteor.common;

/**
 * 
 * Global enumeration to identify the current job status.
 *
 * @author André Greiner-Petter
 *
 */
public enum JobStates {
	// job states
	INITIALIZE("Initialize blank job object."), 
	SUBMIT("Job submitted to scheduler."), 
	WAITING("Job sitting on the waiting queue of the scheduler. Doesn't executed yet."), 
	RUNNING("Job running."),
	FINISHED("Job finished."),
	ERROR("An error occured...");
	
	// string information for each state
	private String message;
	
	/**
	 * Private constructor to set the descriptions
	 * @param code
	 */
	private JobStates( String message ){
		this.message = message;
	}
	
	/**
	 * Returns the statement to this status type.
	 * @return description
	 */
	public String getMessage(){
		return this.message;
	}
}
