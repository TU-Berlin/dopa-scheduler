package eu.stratosphere.meteor.server.executor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import eu.stratosphere.meteor.common.SchedulerConfigConstants;
import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.common.MessageBuilder;

/**
 * This class represents a job on the server site of the DOPAScheduler system.
 * It submits a job object and keep informations about states and results.
 *
 * @author  T.Shan
 * 			André Greiner-Petter
 *
 */
public class RRJob {

	 /**
	  * Identification collection
	  */
	private String clientID;
	private String jobID;
	
	/**
	 * Current job status. If this class instantiated the current status is {@code JobState.WAITING}
	 */
	private JobState status;
	
	/**
	 * An error status as JSONObject
	 */
	private JSONObject errorJSON;
	
	/**
	 * Comes from the MeteorWebfrontend
	 */
	private final ClientFrontend frontend;
	
	/**
	 * Internal informations about script and results (just links)
	 */
	private String mappedScript;
	private List<String> result;
	
	/**
	 * Save the time this job submitted from the client
	 */
	private final Date submitTime;
	
	/**
	 * The executor is a thread to submit the job parallelized to the DOPAScheduler
	 */
	private final JobExecutor executor;
	
	private final Pattern pathPattern = Pattern.compile(
			"(write|read)\\s+(\\$\\w+\\s+to|from)\\s+'\\s*(file:///[^']+|hdfs://[^']+)'\\s*;"
			);
	
	/**
	 * Creates an RoundRobinJob object
	 * @param clientID from the client submitted this job
	 * @param jobID of this job
	 * @param meteorScript included in this job
	 * @param submitTime from client site
	 */
	public RRJob(String clientID, String jobID, String meteorScript, Date submitTime) {
		this.clientID = clientID;
		this.jobID = jobID;
		this.status= JobState.WAITING;
		this.frontend = new ClientFrontend( SchedulerConfigConstants.EXECUTER_CONFIG );
		this.submitTime = submitTime;
		this.result = new ArrayList<String>();
		this.errorJSON = new JSONObject();
		this.executor = new JobExecutor( this );
		this.mappingScript( meteorScript );
	}
	
	// TODO mapping output and input paths of query
	
	private void mappingScript( String meteorScript ){
		Matcher matcher = pathPattern.matcher(meteorScript);
		
		ArrayList<String> outputPaths = new ArrayList<String>();
		
		while ( matcher.find() ){
			if ( matcher.group(1).matches("write") ){
				// TODO mapping this path!
				outputPaths.add( matcher.group(3) );
			} else if ( matcher.group(1).matches("read") ) {
				// TODO
			}
		}
		
		setOutputStrings( outputPaths );
	}
	
	/**
	 * Sets the output links.
	 * @param outputs
	 */
	private void setOutputStrings( List<String> outputs ){
		this.result = outputs;
	}
	
	/**
	 * Creates an error object
	 * @param error
	 */
	protected void setErrorMessage( String error ){
		this.status = JobState.ERROR;
		this.errorJSON = MessageBuilder.buildErrorStatus(clientID, jobID, error);
	}
	
	/**
	 * Sets the status of the job
	 * @param status
	 */
	protected void setStatus( JobState status ){
		this.status = status;
	}
	
	/**
	 * Returns the client frontend which executes the job
	 * @return clientFrontend
	 */
	protected ClientFrontend getClientFrontend(){
		return this.frontend;
	}
	
	/**
	 * Returns the meteor script
	 * @return meteorScript
	 */
	protected String getMeteorScript(){
		return this.mappedScript;
	}
	
	/**
	 * Returns a json object with error informations or null if no error occurred while executing this job.
	 * @return JSONObject with error informations or null if no error occurred
	 */
	public JSONObject getErrorJSON(){
		if ( !status.equals( JobState.ERROR ) ) return null;
		else return this.errorJSON;
	}
	
	/**
	 * Returns current status
	 * @return job status
	 */
	public JobState getStatus(){
		return this.status;
	}
	
	/**
	 * Returns client id
	 * @return clientID
	 */
	public String getClientID() {
		return clientID;
	}

	/**
	 * Returns job id
	 * @return jobID
	 */
	public String getJobID() {
		return jobID;
	}
	
	/**
	 * Returns time when the client submitted that job
	 * @return date
	 */
	public Date getSubmitTime(){
		return submitTime;
	}
	
	/**
	 * Returns the result path of specified index
	 * @param index
	 * @return path of result (null if this result doesn't exists)
	 */
	public String getResult( int index ){
		if ( index < 0 || index >= this.result.size() ) return null;
		return this.result.get(index);
	}
	
	/**
	 * Returns whether this job finished yet.
	 * @return true if this job finished, otherwise false
	 */
    public boolean finished(){
    	return status.equals( JobState.FINISHED ) || status.equals( JobState.ERROR );
	}
    
    /**
     * Runs a new thread to execute the job parallel
     */
	public void execute() {
		this.status = JobState.RUNNING;
		this.executor.start();
	}
	
	/**
	 * Override the method 
	 * @return string representation of this class
	 */
	@Override
	public String toString(){		
		return "RoundRobinJob from Client " + clientID + " with job ID " + jobID + ". Current Status: " + status;
	}
}
