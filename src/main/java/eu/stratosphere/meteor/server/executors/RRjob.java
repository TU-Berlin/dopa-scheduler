package eu.stratosphere.meteor.server.executors;


import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * create a job for each request of State or new job
 * variable attribute, Object RRjob without jobContent/result by requesting status
 * @author T.Shan
 *
 */

//TODO maybe the execute should be called in this class instead of in JobExcecutor
public class RRjob{

	 // a list of lists which include all the jobs from one client 
	public String jType;  
	public String clientID;	   
	String jobID;	   
	public String status;
	
	String jobContent;
	List<String> result;
	
	   
	public RRjob(String jobType, String cid, String jid) {
	      this.jType=jobType;
	      this.clientID=cid;
	      this.jobID=jid;	    
	      this.status="open";
	}
	
	public String getStatus(){
		return this.status;
	}
	
	public void resetStatus(){
		this.status="finished";
	}
	
	public String getjType() {
		return jType;
	}

	public String getClientID() {
		return clientID;
	}

	public String getJobID() {
		return jobID;
	}

	
	public void setContent(String jid, String meteorScript){
		if (this.jobID.equals(jid)){
			this.jobContent=meteorScript;
		}
	}
		
	public String getResult(){
		String answer=null;
		
		if (this.isDone()){
			answer=result.toString();
		}
		return answer;		
	}
	  
    public boolean isDone( ){
    	if (this.status.equals("finished")){
    		return true;
    	}else{
    		return false;
    	}
	}	

	public void execute() {
		
		//execute the metoer-script		
		ClientFrontend frontEnd =new ClientFrontend (null);
		
		try {
			frontEnd.execute(this.jobContent);
			this.result=frontEnd.getOutputPaths();
			this.resetStatus();
			
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
		
		output=jType+"."+clientID+"."+jobID;		
		return output;
		
	}

}
