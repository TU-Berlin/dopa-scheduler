package eu.stratosphere.meteor.server.executors;

import java.util.LinkedList;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * This class organize received jobs under each client. 
 * It collects all received jobs into a linked list and 
 * submits the list as a round robin algorithm.
 *
 * @author  T.Shan
 */
public class ClientObject implements Comparable<ClientObject>{
	
	private String clientID;
	private LinkedList<RRjob> waitingJobs;
	private LinkedList<RRjob> results;
	
	public ClientObject(String id) {
		this.clientID=id;
	}

    //set up a new incoming job from this client
	public void addJob(RRjob job){
		 boolean foundJobInList=false; 
		 for (RRjob eachJob:waitingJobs){
			 
			 if(eachJob.getJobID().equals(job.jobID)){
				 foundJobInList=true;
				 System.out.println("This Job is in the waitinglist already");
			 }
		 }
		 if(!foundJobInList){
			 waitingJobs.addLast(job);
		 }
		
	}


	public String getClientID() {
		return clientID;
	}


	public LinkedList<RRjob> getWaitingJobs() {
		return waitingJobs;
	}


	//get all job records of a client
	public LinkedList<RRjob> getResults() {
		return results;
	}


	public void setClientID(String clientID) {
		this.clientID = clientID;
	}	

	//check out, if there's jobs waiting from this client
	public boolean waitingListEmpty() {
		if (this.waitingJobs.isEmpty()){
			return true;
		}else{
			return false;
		}
	}


	//receive and delete first job in the waiting list
	public RRjob getfirstJob() {
		RRjob output;
		output=waitingJobs.pollFirst();
		return output;
	}


	//add one finished job with result to the result-list
	public void addResult(RRjob job) {
		boolean foundJobInList=false; 
		 for (RRjob eachJob: results){
			 
			 if(eachJob.getJobID().equals(job.jobID)){
				 foundJobInList=true;
				 System.out.println("This Job is in the waitinglist already");
			 }
		 }
		 if(!foundJobInList){
			 results.addLast(job);
		 }
		
	}
	
	//find the status of one job from the client
	private JSONObject getJobStatus(RRjob job) throws JSONException{
		JSONObject status = null;
		RRjob ref=job;		 
				 
		for (RRjob jobR:results){			
			 if (jobR.getClientID().equals(ref.getClientID())){
					
				 status.append("State", jobR.getStatus());
				 
			  }
		 }
		for (RRjob jobR:waitingJobs){
			if (jobR.getClientID().equals(ref.getClientID())){
				
				 status.append("State", jobR.getStatus());
				
			  }
		}
		return status;
	}

	@Override
	public int compareTo( ClientObject obj ) {
		if ( this.clientID.matches( obj.getClientID() ) ) return 0;
		else return 1;
	}
	
/*	private  LinkedList<RRjob> getNextList(){
		LinkedList<RRjob> tmpClient;
		int next = 0;
	
		  for (int i=0; i<allJobs.size();i++){
			tmpClint=allJobs.get(i);
			
				RRjob firstJob= tmpClint.get(0);
				String[] clientList=(String[]) firstJob.toString().split(".");
				
				if (clientList[0].equals(clientID)){
					next=i+1;
					
				}
			}
		return allJobs.get(next);
	}*/
}