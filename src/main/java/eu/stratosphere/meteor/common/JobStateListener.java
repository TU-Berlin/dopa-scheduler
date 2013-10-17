package eu.stratosphere.meteor.common;

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
	 * @param status
	 */
	public void stateChanged( DSCLJob job, DSCLJob.State status );
}
