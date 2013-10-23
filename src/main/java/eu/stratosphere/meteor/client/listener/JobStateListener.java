package eu.stratosphere.meteor.client.listener;

import eu.stratosphere.meteor.client.DSCLJob;
import eu.stratosphere.meteor.common.JobState;

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
	public void stateChanged( DSCLJob job, JobState newStatus );
}
