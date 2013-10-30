package eu.stratosphere.meteor.server.executors;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * create a job for each request of State or new job
 * variable attribute, Object RRjob without script/result by requesting status
 * @author T.Shan
 *
 */

//TODO maybe the execute should be called in this class instead of in JobExcecutor
public class RRjob{

	 // a list of lists which include all the jobs from one client
	public String clientID;	   
	String jobID;	   
	private JobState status;
	
	private ClientFrontend frontend;
	
	String script;
	
	List<String> result;
	
	private final Date submitTime;
	
	public RRjob(String clientID, String jobID, String meteorScript, Date submitTime) {
	      this.clientID = clientID;
	      this.jobID = jobID;
	      this.status= JobState.WAITING;
	      this.script = meteorScript;
	      this.frontend = new ClientFrontend(null);
	      this.submitTime = submitTime;
	      this.result = new ArrayList<String>();
	      
	      result.add("Wie geil is das denn");
	}
	
	public JobState getStatus(){
		return this.status;
	}
	
	public void resetStatus( JobState status ){
		this.status = status;
	}
	
	public String getClientID() {
		return clientID;
	}

	public String getJobID() {
		return jobID;
	}
		
	public String getResult( int index ){
		if ( index < 0 || index >= this.result.size() ) return null;
		return this.result.get(index);
	}
	  
    public boolean isDone( ){
    	if (this.status.equals("finished")){
    		return true;
    	}else{
    		return false;
    	}
	}	

	public void execute() {
		
		try {
			this.frontend.execute(this.script);
			this.result = frontend.getOutputPaths();
			this.resetStatus( JobState.FINISHED );
		} catch (QueryParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	public String toString(){
		String output=new String();
		
		output=clientID+"."+jobID;		
		return output;
		
	}

}
