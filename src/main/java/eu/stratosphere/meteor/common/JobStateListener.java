package eu.stratosphere.meteor.common;

import eu.stratosphere.meteor.client.DSCLJob;

/**
 * TODO tutorial/descriptions how to handle this interface
 * 
 * @author André Greiner-Petter
 *
 */
public interface JobStateListener {
	/**
	 * 
	 * @param job
	 * @param newStatus
	 */
	public void stateChanged( DSCLJob job, JobStates newStatus );
}
