package eu.stratosphere.meteor.common;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mleich on 11/12/13.
 */
public interface DSCLJob {

    /**
     * Returns the map of all results saved on this job.
     * @return a modifiable map of results
     */
    public HashMap<Integer, File> getResults();

    /**
     * Returns the current status of this job.
     * @return current job status
     */
    public JobState getStatus();

    /**
     * Returns the ID of this object.
     * @return ID
     */
    public String getID();

    /**
     * @return meteor script
     */
    public String getMeteorScript();

    /**
     * Return the link of a specified result.
     * @return link
     */
    public String getResultLink( int index );

    /**
     * Returns an unmodifiable list of all result handlers currently executing on this job.
     * @return map with key for file index and the specified handler
     */
    public Map<Integer, ResultFileHandler> getResultHandler();

    /**
     * Returns an unmodifiable list of all listeners of this job.
     * @return list of all listeners
     */
    public List<JobStateListener> getListeners();

    /**
     * Adds a new listener.
     * @param listener
     * @return true if the collection changed
     */
    public boolean addJobStateListener( JobStateListener listener );

    /**
     * Try to remove the specified listener.
     * @param listener
     * @return true if the collection changed
     */
    public boolean removeJobStateListener( JobStateListener listener );

    /**
     * Sends a request to get specified blocks of result file by specified index.
     * @param fileIndex of result file
     * @param desiredBlockSize size of block you want for one block, scheduler can choose own sizes if necessary
     * @param maxNumberOfBlocks threshold for blocks
     * @param handler to handle each incoming block and put them all together
     */
    public void requestResult( int fileIndex, int desiredBlockSize, long maxNumberOfBlocks, ResultFileHandler handler );

    /**
     * Get HDFS path of output file for use in follow-up jobs. You specified the link by a given
     * index. The connection factory add the link automatically after received.
     * @param fileIndex specified index
     */
    public void getLink( int fileIndex );

    /**
     * Its abort this specified job on the scheduler site. This job is deleted on scheduler site when the current
     * status changed to {@code JobState.DELETED}.
     */
    public void abortJob();

}
