package eu.stratosphere.meteor.client.job;

import eu.stratosphere.meteor.client.DSCLJob;


/**
 * This interface is responsible for handling the individual blocks that get delivered to the client.
 *
 * @author André Greiner-Petter
 * 
 */
public interface ResultFileHandler {
	/**
	 * Handle incoming file blocks. This method invokes asynchronously if
	 * a new file block received.
	 * 
	 * @param job specified DSCLJob waits for the result
	 * @param block of huge file
	 */
	public void handleFileBlock( DSCLJob job, ResultFileBlock block );
}
