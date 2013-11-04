package eu.stratosphere.meteor.server;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import eu.stratosphere.meteor.SchedulerConfigConstants;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.common.MessageBuilder;
import eu.stratosphere.meteor.common.MessageBuilder.RequestType;
import eu.stratosphere.meteor.server.executor.FileSender;
import eu.stratosphere.meteor.server.executor.RRJob;
import eu.stratosphere.meteor.server.executor.RoundRobin;

/**
 * The scheduler will started by Server.java. The scheduler connects to
 * the job queue (a RabbitMQ queue) and wait for jobList. If the scheduler
 * submits received jobList and sends the status of each job to an exchange
 * queue.
 *
 * @author Andre Greiner-Petter
 *         T.shan
 *         Etienne Rolly
 */
public class DOPAScheduler {
	/**
	 * Log for server site.
	 */
	public static Log LOG = LogFactory.getLog( DOPAScheduler.class );
	
	/**
	 * Factory to handle all connections with rabbitMQ
	 */
	private ServerConnectionFactory connectionFactory;
	
	/**
	 * Collection of all jobs to iterate through while working process.
	 * Each element is just a reference to job object in workingJobsCollection or in finishedJobsCollection.
	 */
	private RoundRobin<String, RRJob> roundRobinJobList;
	
	/**
	 * Map about clients and their jobs. Identified by clientID and jobID.
	 * <clientID, <jobID, JobObject>>
	 */
	private HashMap<String, HashMap<String, RRJob>> workingJobsCollection;
	
	/**
	 * Same like workingJobsCollection above. Filled with finished jobs.
	 */
	private HashMap<String, HashMap<String, RRJob>> finishedJobsCollection;
	
	/**
	 * Paused main-loop flag
	 */
	private boolean paused = false;
	
	/**
	 * Saves the current thread
	 */
	private final Thread schedulerThread = Thread.currentThread();
	
	/**
	 * A global constant about the working time for each cycle
	 */
	private final long WAITING_TIME = 100;
	
	/**
	 * If you want to get a DOPAScheduler object please use the static method to create
	 * once. Note that only one client per system is allowed.
	 */
	private DOPAScheduler() {
		this.roundRobinJobList = new RoundRobin<String, RRJob>();
		this.workingJobsCollection = new HashMap<String, HashMap<String, RRJob>>();
		this.finishedJobsCollection = new HashMap<String, HashMap<String, RRJob>>();
	}
	
	/**
	 * Connects the scheduler with the rabbitMQ system to handle all traffic from and to clientList.
	 */
	private void connect() {
		this.connectionFactory = new ServerConnectionFactory( this );
	}
	
	/**
	 * Adds a new incoming job. If a job with same identifications still exists it will be overwrite it.
	 * @param clientID specified client
	 * @param jobID specified job
	 * @param properties from request
	 * @param script of this job
	 */
	private void handleIncomingJob( String clientID, String jobID, BasicProperties properties, byte[] script ){
		DOPAScheduler.LOG.info("New job received. JobID: " + jobID );
		
		// is this job in working list yet
		RRJob job = getWorkingJob( clientID, jobID );
		if ( job != null ) { // if yes delete it
			job = workingJobsCollection.get( clientID ).remove(jobID);
			roundRobinJobList.remove(clientID, job);
		}
		
		// is this job still finished
		job = getFinishedJob( clientID, jobID );
		// delete it
		if ( job != null ) job = finishedJobsCollection.get(clientID).remove(job);
		
		try {
			// create new job
			String encoding = properties.getContentEncoding();
			String meteorScript = new String( script, encoding );
			Date submitTime = properties.getTimestamp();
			
			job = new RRJob( clientID, jobID, meteorScript, submitTime );
			
			// put to existing list or create once
			HashMap<String, RRJob> clients = workingJobsCollection.get(clientID);
			if ( clients == null ) clients = workingJobsCollection.put(clientID, new HashMap<String, RRJob>());
			clients.put(jobID, job);
			
			// added to working list
			roundRobinJobList.add( clientID, job );
			
			// send new job status to client
			statusUpdate( clientID, jobID );
		} catch ( UnsupportedEncodingException uee ){
			LOG.error( "Cannot add a new DSCLJob, encode given script failed with false encoding informations.", uee );
		} catch ( NullPointerException npe ){
			LOG.error( "Any informations are null. Cannot handle incoming job.", npe );
		}
	}
	
	/**
	 * Handle an incoming delivery by find out request type and reply that request.
	 * @param delivery incoming message
	 * @throws UnsupportedEncodingException cannot encrypted by given encoding type
	 * @throws JSONException if any argument is null
	 */
	private void handleIncomingRequest( Delivery delivery ) 
			throws UnsupportedEncodingException, JSONException {		
		//checks if the incoming delivery contains a json string
		if( !delivery.getProperties().getContentType().contains("json") ){
			LOG.warn("Incoming message doesn't contains a json string.");
			return;
		}
		
		//get the encoding type from the delivery and with it the string from the body of our delivery
		String encodingType = delivery.getProperties().getContentEncoding();
		JSONObject request = new JSONObject( new String( delivery.getBody(), encodingType ) );
		
		// get root informations of request
		String clientID = MessageBuilder.getClientID(request);
		String jobID = MessageBuilder.getJobID(request);
		
		// handle specific request
		switch ( RequestType.getRequestType(request) ){
			case JOB_STATUS: // same as JOB_EXISTS request
			case JOB_EXISTS:
				statusUpdate( clientID, jobID ); 
				break;
			case GET_LINK:
				replyLink( clientID, jobID, request, delivery.getProperties() ); 
				break;
			case REQUEST_RESULT: 
				sendResult( clientID, jobID, delivery );
				break;
			case JOB_ABORT: 
				abortJob( clientID, jobID ); 
				break;
			default: // false request
		} // end switch-case
	}
	
	/**
	 * Sends a status update to specified client of its job.
	 * @param clientID specified client
	 * @param jobID specified job
	 */
	private void statusUpdate( String clientID, String jobID ){
		//get the specified RRjob from the workingJobsCollection by the ClientID and the JobID
		RRJob job = getWorkingJob( clientID, jobID );
		if ( job == null ) job = getFinishedJob( clientID, jobID );
		
		// build json object for reply
		JSONObject jobStatus;
		if ( job != null ) jobStatus = MessageBuilder.buildJobStatus( clientID, jobID, job.getStatus() );
		else jobStatus = MessageBuilder.buildJobStatus( clientID, jobID, JobState.DELETED );
		
		// send reply
		try { this.connectionFactory.sendJobStatus( clientID, jobStatus ); }
		catch ( IOException ioe ){ LOG.error("Cannot send status update.", ioe); }
	}
	
	/**
	 * Send the link of a finished job to client.
	 * @param clientID specified client
	 * @param jobID specified job
	 * @param request object from client
	 * @param properties from request
	 */
	private void replyLink( String clientID, String jobID, JSONObject request, BasicProperties properties ){
		//get the specified RRjob from the workingJobsCollection by the ClientID and the JobID
		RRJob job = getFinishedJob( clientID, jobID );
		
		// get index from request
		int idx = MessageBuilder.getFileIndex( request );
		
		// build reply
		JSONObject reply = MessageBuilder.buildGetLink(clientID, jobID, idx);
		
		if ( job != null ) reply = MessageBuilder.addPath( reply, job.getResult(idx) );
		else reply = MessageBuilder.addPath( reply, "" );
		// TODO have to send error message or something that can handled
		
		// send message
		try { this.connectionFactory.replyRequest( properties, reply ); }
		catch ( IOException ioe ){ LOG.error( "Cannot send link of job.", ioe ); }
	}
	
	/**
	 * Abort the given job and send a status update (deleted) to client.
	 * @param clientID specified client
	 * @param jobID specified job
	 */
	private void abortJob( String clientID, String jobID ){
		//get the specified RRjob from the workingJobsCollection by the ClientID and the JobID
		HashMap<String, RRJob> clientMap = workingJobsCollection.get(clientID);
		HashMap<String, RRJob> finClientMap = workingJobsCollection.get(clientMap);
		
		// if exists, delete it
		if ( clientMap != null ) clientMap.remove(jobID);
		if ( finClientMap != null ) finClientMap.remove(jobID);
		
		// send new status
		JSONObject reply = MessageBuilder.buildJobStatus( clientID, jobID, JobState.DELETED );
		try { this.connectionFactory.sendJobStatus(clientID, reply); }
		catch ( IOException ioe ) { LOG.error( "Cannot send deleted job status, after deleted job.", ioe ); }
		
		DOPAScheduler.LOG.info("Job aborted. JobID: " + jobID);
	}
	
	/**
	 * TODO
	 * @param clientID
	 * @param jobID
	 * @param delivery
	 */
	private void sendResult( String clientID, String jobID, Delivery delivery ){
		RRJob job = this.finishedJobsCollection.get(clientID).get(jobID);
		if ( job == null ) return; // TODO any error message to resultConsumer
		
		// TODO multi-threading!
		FileSender sender = new FileSender( this.connectionFactory, job, delivery );
		sender.run();
	}
	
	/**
	 * Test whether specified job contains in working list. If it is so it returns the job object,
	 * otherwise returns null.
	 * @param clientID
	 * @param jobID
	 * @return RRjob if its exists, otherwise null
	 */
	private RRJob getWorkingJob( String clientID, String jobID ){
		RRJob job = null;
		HashMap<String, RRJob> clientMap = workingJobsCollection.get(clientID);
		if ( clientMap != null ) job = clientMap.get(jobID);
		return job;
	}
	
	/**
	 * Test whether specified job contains in finished list. If it is so it returns the job object,
	 * otherwise returns null.
	 * @param clientID
	 * @param jobID
	 * @return RRjob if its exists, otherwise null
	 */
	private RRJob getFinishedJob( String clientID, String jobID ){
		RRJob job = null;
		HashMap<String, RRJob> clientMap = finishedJobsCollection.get(clientID);
		if ( clientMap != null ) job = clientMap.get(jobID);
		return job;
	}
	
	/**
	 * TODO
	 * work on the jobList from job list in round robin for WORKING_TIME seconds
	 */
	private void workOnJobs(){
		//take first job from the job-list and its status
		RRJob currentJob = roundRobinJobList.next();
		
		if ( currentJob == null ){
			// clean garbage
			roundRobinJobList.hardReset();
			return;
		}
		
		//execute the current job
		//currentJob.execute();
		
		// TODO TEST
		HashMap<String, RRJob> tmpMap = new HashMap<String, RRJob>();
		tmpMap.put(currentJob.getJobID(), currentJob);
		this.finishedJobsCollection.put(currentJob.getClientID(), tmpMap);
	}
	
	/**
	 * Adds an incoming client to the scheduler services. Returns true if
	 * the client got the rights to enter this service, false otherwise.
	 * 
	 * @param clientID 
	 * @return true if the client got the rights, false otherwise
	 */
	protected boolean addClient(String clientID) {
		if ( workingJobsCollection.containsKey(clientID) ) return false;
		else {
			workingJobsCollection.put(clientID, new HashMap<String, RRJob>());
			finishedJobsCollection.put( clientID, new HashMap<String, RRJob>());
			DOPAScheduler.LOG.info("Client '" + clientID + "' registered.");
		}
		return true;
	}
	
	/**
	 * Removes client with all connections to jobs. Returns true if it worked, otherwise false.
	 * @param clientID
	 * @return true if client removed well, false otherwise
	 */
	protected boolean removeClient( String clientID ){
		// TODO allowed? if not return false
		
		// remove client and all jobs from this client from working list
		roundRobinJobList.remove(clientID);
		
		//HashMap<String, RRJob> map = workingJobsCollection.remove(clientID);
		//for ( String key : map.keySet() ) jobList.remove( map.get(key) );
		
		// remove finished jobs as well
		finishedJobsCollection.remove(clientID);
		
		// finished
		return true;
	}
	
	/**
	 * Starts the main loop of the scheduler. Handle deliveries like requests or new jobList
	 * and execute other jobList with the round robin algorithm. Inform clientList about new job
	 * states and possibly error messages.
	 * You can pause the system by using pause() and restart it with restart(). If you paused
	 * the system you cannot invoke this method to restarts the server. Please use restart().
	 */
	public void start() {
		
		// main loop handle incoming, outgoing messages and work through job lists
		while( !paused ){
			
			// get delivery
			Delivery delivery = connectionFactory.getRequest( WAITING_TIME );
			
			// if nothing todo at all, sleep a bit and continue after wake up
			if ( delivery == null && !roundRobinJobList.hasNext() ){
				try { Thread.sleep( WAITING_TIME ); } 
				catch (InterruptedException e) { Thread.interrupted(); }
				continue;
			}
			
			if ( delivery != null ){
				//get the rountingKey from the delivery
				String routingKey = delivery.getEnvelope().getRoutingKey();
				
				// if incoming message is a request
				if ( routingKey.matches( SchedulerConfigConstants.REQUEST_KEY_MASK ) ){
					try { handleIncomingRequest( delivery ); } 
					catch (UnsupportedEncodingException e) { LOG.error("Cannot decrypt incoming request.", e); }
					catch (JSONException e) { LOG.error("Unbelievable. Send me how you produces this error...", e); }
				} else { // else search for jobs
					String[] separateKey = routingKey.split("\\.");
					if ( separateKey[0].matches("setJob") && separateKey.length >= 3 )
						handleIncomingJob( separateKey[1], separateKey[2], delivery.getProperties(), delivery.getBody() );
				}
			} // end if delivery != null
			
			/**
			 * execute jobList via round robin algorithm here
			 */
			workOnJobs();
			
			/**
			 * TODO
			 * inform clientList about new states or some other stuff here
			 */
			
			/**
			 * TODO
			 * possibly traffic with hadoop here
			 */
			
			/**
			 * TODO all other stuff here
			 * maybe clean garbage like delete finished jobList or something like that
			 * inform clientList if you want to delete something, we have to discuss it as well
			 */
			
			// System yield, to keep this time as short as possible use setSchedulerPriority( int priority )
			Thread.yield();
		}
	}
	
	/**
	 * It restarts the system after you paused the scheduler.
	 */
	public void restart(){
		if ( !paused ) return;
		this.paused = false;
		start();
	}
	
	/**
	 * Paused the scheduler. The scheduler finished last loop cycle and stopped until you restarts
	 * the server. (Call restart() to do this)
	 */
	public void pause(){
		this.paused = true;
	}
	
	/**
	 * If you want to power up the scheduler on your system it's possible to
	 * push the priority of the scheduler thread. That's the best solution
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
	 * Stops the scheduler service by shutdown all connections with RabbitMQ and close the
	 * ServerConnectionFactory.
	 * 
	 * @throws IOException
	 */
	public void shutdown() throws IOException {
		this.connectionFactory.shutdownConnections();
	}
	
	/**
	 * Creates and return a new Scheduler object. It just initialize all connections and create objects to
	 * handle clientList and jobList.
	 * The returned scheduler doesn't work yet. You have to start the service to invoke start(). This starts
	 * the loop of the service to handle all interactions. If you want to pause the scheduler without shutdown
	 * you can call the pause() method. If you want to restart your system please use restart() method.
	 * 
	 * @return DOPAScheulder object in pause mode.
	 */
	public static DOPAScheduler createNewSchedulerSystem(){
		DOPAScheduler scheduler = new DOPAScheduler();
		scheduler.connect();
		return scheduler;
	}
	
	/**
	 * TODO we have to discuss the best initialization way on the scheduler site.
	 * @param args
	 * @throws UnsupportedEncodingException
	 */
	public static void main( String[] args ){
		DOPAScheduler scheduler = createNewSchedulerSystem();
		scheduler.start();
	}
}
