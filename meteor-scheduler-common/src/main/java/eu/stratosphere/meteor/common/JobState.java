package eu.stratosphere.meteor.common;

/**
 * 
 * Global enumeration to identify the current job status.
 *
 * @author André Greiner-Petter
 *
 */
public enum JobState {
	// job states
	UNDEFINED("Undefined job status."), // undefined means it has no script yet
	DELETED("Deleted from scheduler."), // only possible after requested whether this job exists
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
	 * @param message
	 */
	private JobState( String message ){
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
