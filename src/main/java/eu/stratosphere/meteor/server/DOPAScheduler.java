package eu.stratosphere.meteor.server;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.LinkedList;

import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import eu.stratosphere.meteor.SchedulerConfigConstants;
import eu.stratosphere.meteor.common.DSCLJob;
import eu.stratosphere.meteor.server.executors.ClientObject;
import eu.stratosphere.meteor.server.executors.RRjob;

/**
 * The scheduler will started by Server.java. The scheduler connects to
 * the job queue (a RabbitMQ queue) and wait for jobs. If the scheduler
 * submits received jobs and sends the status of each job to an exchange
 * queue.
 *
 * @author André Greiner-Petter
 *         T.shan
 */
public class DOPAScheduler {
	
	/**
	 * Factory to handle all connections with rabbitMQ
	 */
	private final ServerConnectionFactory connectionFactory;
	
	/**
	 * new received jobs will be saved in allJobs
	 * finished jobs will be saved in allResults
	 */
	private LinkedList<LinkedList<RRjob>> allResults;
	private LinkedList<ClientObject> clients;
	
	/**
	 * Saves the current thread
	 */
	private final Thread schedulerThread = Thread.currentThread();
	
	/**
	 * Create a new scheduler and connect it to all queues.
	 */
	protected DOPAScheduler() {
		this.connectionFactory = new ServerConnectionFactory();
		this.clients = new LinkedList<ClientObject>();
		this.allResults = new LinkedList<LinkedList<RRjob>>();
	}
	
	/**
	 * If you want to power up the scheduler on your system it's possible to
	 * push up the priority of the scheduler thread. That's the best solution
	 * to keep the time between two cycles (of main loop) as short as possible.
	 * 
	 * @param priority can be a value between Thread.MIN_PRIORITY and Thread.MAX_PRIORITY
	 */
	public void setSchedulerPriority( int priority ){
		if ( priority < Thread.MIN_PRIORITY ) priority = Thread.MIN_PRIORITY;
		if ( priority > Thread.MAX_PRIORITY ) priority = Thread.MAX_PRIORITY;
		schedulerThread.setPriority( priority );
	}
	
	/**
	 * @throws UnsupportedEncodingException 
	 * 
	 */
	public void start() throws UnsupportedEncodingException {
		String jobType = null;
		String clientID = null;
		String jobID = null;
		String message = null;
		
		System.out.println( "[X Scheduler] Process started. Waiting for jobs...\n" );
		
		while( true ){
			QueueingConsumer.Delivery delivery = connectionFactory.getRequest(100);
			
			// TODO
			if (delivery == null) continue;
			
			String[] routingKey = delivery.getEnvelope().getRoutingKey().split("\\.");
			
			//System.out.println( "Got message with routing key: " + Arrays.toString( routingKey ) );
			
			// no registration
			if ( routingKey[0].matches("register") ){
				String name = new String( delivery.getBody(), delivery.getProperties().getContentEncoding() );
				if ( routingKey[1].matches("login") ) {
					System.out.println( "Add new client with name: " + name );
					this.clients.add( new ClientObject( name ) );
				}
				//else this.clients.remove( new ClientObject( name ) ); //TODO
			}
			
			JSONObject obj = new JSONObject();
			try {
				obj.put("JobID", "123");
				obj.put("State", DSCLJob.State.RUNNING);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				connectionFactory.sendJobStatus(this.clients.getFirst().getClientID(), obj);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/*
			//get relevant informations from queue(delivery)
			message = new String( delivery.getBody() );			
			String routingKey = delivery.getEnvelope().getRoutingKey();
			
			String[] routingKeyArr=routingKey.split("\\.");
			
			
						 
			 //finished the saved jobs inbetween
			 work();
			 
			 // the scheduler finished for this "round" of processes (message to CPU)
			 Thread.yield();*/
		}
	}
	
	public void stop() throws IOException {
		this.connectionFactory.shutdownConnections();
	}
	
	protected void addClient(String clientID) {
		// TODO
		boolean foundClient=false; 
		 for (ClientObject eachClient:clients){
			 ClientObject tmp=eachClient;
			 if (tmp.getClientID().equals(clientID)){
			
				  foundClient=true;
				  System.out.println("Client is already registered.");
			  }
		 }
		 if(foundClient==false){
			 clients.addLast(new ClientObject(clientID));
		 }
	}

	
	//record new jobs to client
	private synchronized void addJob(RRjob jobIn) {
		
		 boolean foundJobList=false; 
		 for (ClientObject anyClient:clients){	
		
			 //check out client-ID
							  
			 if (anyClient.getClientID().equals(jobIn.clientID)){
						  
				 anyClient.addJob(jobIn);							  
				 foundJobList=true;	  
						  
				 System.out.println("New job: "+jobIn.toString()+" is added in the waiting-list");
						  
			 }
		}
		// in case there is no relevant job list jet
		  if(!foundJobList){
			  ClientObject clientN =new ClientObject(jobIn.getClientID());
			  clientN.addJob(jobIn);
			  this.clients.add(clientN);
			  
			  System.out.println("New client's added in the waiting-list. JobID: "+jobIn.toString());
			  
		  }		  
	}
	
	
	/**
	 * work in Round-Robin: each client may only execute one job once 
	 * get the first List<String> from LinkedList<LinkedList<String>> jobs 
	 * , which would be deleted at the same time 
	 * execute the content part of first String, and deleted this from List
	 * add this List to LinkedList<LinkedList<String>> jobs again
	*/
	@SuppressWarnings("unchecked")
	private synchronized void work(){
	
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
		
		//System.err.println( "work with: " + allJobs );
		
		while(!clients.isEmpty()){
			//get the job-list from the first Client,
			//get first list<jobs> from the whole list<list<jobs>>
			
			ClientObject currentClient=clients.getFirst();			
					
			System.out.println("working on first client: "+currentClient);
			
			//keep the client with empty job list and put it in the end			
			if(((ClientObject) currentClient).waitingListEmpty()){
				clients.pop();
				clients.add(currentClient);
				
			}else{
				/* execute the meteor script from current client
				 * get and delete the executed job, interrupted job would be lost 
				 * other elements from the list still being available,
				 */
				RRjob activJob=(currentClient.getfirstJob());
				
				System.out.println("Working on: "+activJob.toString());

				//TODO this if-block(requestStatus) is kept only for test phase				
				//The Jobs in allResult may also be asked for status
				if(activJob.getjType().equals("requestStatus")){
					
					//getJobStauts(aktivJob);
					activJob.getStatus();
					
					System.out.println("jobID: "+activJob.toString()+" State: "+activJob.getStatus());
					
					//replace the client to the last position in the client list
					clients.removeFirst();
					clients.add(currentClient);
					
				}else{ 
					//if(aktivJob.getjType().equals("setJob")
					System.out.println("execute meteorScript");
					activJob.execute();	
					
					//save the results in another list
					addResult(activJob,currentClient);
					
					//push the new status to status-queue
					JSONObject status= new JSONObject();
					
					try {
						status.append("status", activJob.getStatus());
							this.connectionFactory.sendJobStatus(activJob.getClientID(), status);
					} catch (IOException | JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					//replace the client to the last position
					clients.removeFirst();
					clients.add(currentClient);				
					
					System.out.println("working finished: "+activJob.toString());
				}
			}
						
		}
				
	}
	//finished job is saved the the list allResult
		private void addResult(RRjob jobResult, ClientObject client) {
					
			 //check the client ID			 
			  if (client.getClientID().equals(jobResult.getClientID())){
					  client.addResult(jobResult);	
					  		
			  }
		}	

		/*
		 * reload the result to client by given jobID/clientID
		 */
		private String getResult(String clientID, String jobID) {
			
			String foundResult="Job is't finished.";
			for (ClientObject anyClient:clients){
				  
				  //check the client ID
							  
				  if (anyClient.getClientID().equals(clientID)){
					  ClientObject c=anyClient;
					  for (RRjob jOfClient:c.getResults()){
						  if(jOfClient.getJobID().equals(jobID)){
							  foundResult=jOfClient.getResult();
						  }
					  }				
				  }
			  }
			return foundResult;	
		}
		

		
	public static void main( String[] args ) throws UnsupportedEncodingException{
		DOPAScheduler scheduler = new DOPAScheduler();
		scheduler.start();
	}
}
