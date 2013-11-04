package eu.stratosphere.meteor.common;

import eu.stratosphere.meteor.client.DSCLJob;

/**
 * This interface represents a listener for status changes of DSCLJobs. If you
 * want to submit (or create) a new DSCLJob to execute on the scheduler you can
 * add one or more JobStateListeners to a job object. Of course you can add one
 * JobStateListener to more jobs then one. For this case the method stateChanged
 * given a DSCLJob to specify the job.
 * 
 * The method stateChanged will invoke asynchronously instant a new status arrived
 * the connection factory. This call all listeners of incoming jobs.
 * 
 * @author André Greiner-Petter
 *
 */
public interface JobStateListener {
	/**
	 * Called when the state of the associated job changed.
	 * @param job specified job object ( if one listener listen on many jobs )
	 * @param newStatus new JobState of specified job
	 */
	public void stateChanged( DSCLJob job, JobState newStatus );
}
