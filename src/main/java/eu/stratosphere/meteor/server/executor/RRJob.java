package eu.stratosphere.meteor.server.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.sopremo.query.QueryParserException;

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
	 * Comes from the MeteorWebfrontend
	 */
	private final ClientFrontend frontend;
	
	/**
	 * Internal informations about script and results (just links)
	 */
	private String script;
	private List<String> result;
	
	/**
	 * Save the time this job submitted from the client
	 */
	private final Date submitTime;
	
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
	      this.script = meteorScript;
	      this.frontend = new ClientFrontend(null); // TODO arguments?
	      this.submitTime = submitTime;
	      this.result = new ArrayList<String>();
	      
	      // TODO dummy
	      this.result.add("Im_a_test_path!");
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
	 * Returns true if this job finished
	 * @return
	 */
    public boolean isDone( ){
    	if (this.status.equals("finished")){
    		return true;
    	}else{
    		return false;
    	}
	}

	public void execute() {
		// TODO start an execution thread
		try {
			this.frontend.execute(this.script);
			this.result = frontend.getOutputPaths();
			this.status = JobState.FINISHED;
		} catch (QueryParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
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
