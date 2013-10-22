package eu.stratosphere.meteor.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

import eu.stratosphere.meteor.common.ClientRequests;
import eu.stratosphere.meteor.common.JobStateListener;
import eu.stratosphere.meteor.common.JobStates;

/**
 * 
 * This class represents the DOPA API client. Each client that connects
 * to the scheduler needs to have a unique clientID. You can create a client
 * by call the static method {@code createNewClient( final String clientID )}.
 * 
 * Before you can use this client object you have to connect it to the
 * scheduler, which is done using its {@code connect} method. Call any
 * other method before the client is connected an exception being thrown.
 * 
 * To see status informations about the client it implements a logger from
 * {@link org.apache.commons.logging.Log}.
 *
 * @author 	André Greiner-Petter
 * 			Tieyan Shan
 *			Etienne Rolly
 */
public class DOPAClient {
	/**
	 * The log for client site.
	 */
	public static final Log LOG = LogFactory.getLog( DOPAClient.class );
	
	/**
	 * The unique clientID of this client object
	 */
	private final String clientID;
	
	/**
	 * The connection factory to handle all traffic from and to this client
	 */
	private ClientConnectionFactory connectionFac;
	
	/**
	 * A map of jobs from this client. The key represents the job ID.
	 */
	private HashMap<String, DSCLJob> jobs;
	
	/**
	 * Constructs a new client object. This client isn't connected
	 * to scheduler yet.
	 * @param clientID final unique identifier
	 */
	private DOPAClient( final String ID ) {
		this.clientID = ID;
		this.connectionFac = null;
		this.jobs = new HashMap<String, DSCLJob>();
	}
	
	/**
	 * Calculate a random ID.
	 * @return random ID
	 */
	private String getRandomID(){
		return java.util.UUID.randomUUID().toString();
	}
	
	/**
	 * Returns the unique ID of this client object.
	 * @return ID
	 */
	public String getClientID(){
		return this.clientID;
	}
	
	/**
	 * Try to connect the client with the scheduler services.
	 * If this failed for any reason you can try it again.
	 * 
	 * It do nothing if the client is still connected.
	 */
	public void connect() {
		// if the client is still connect
		if ( this.connectionFac != null ) {
			LOG.info( "The client is still connected. If you want to reconnect the client disconnect it first." );
			return;
		}
		
		// else try to connect it
		try { this.connectionFac = new ClientConnectionFactory( this ); }
		catch (Exception exc) { /*LOG.error( "Cannot connected to the scheduler services: " + exc.getMessage(), exc );*/ }
	}
	
	/**
	 * Try to disconnect the client. 
	 * It do nothing if the client isn't connected yet.
	 * 
	 * If this method failed for any reason you can try it again.
	 */
	public void disconnect() {
		// is the client connected?
		if ( this.connectionFac == null ){
			LOG.warn("The client isn't connected. Please connect it first.");
			return;
		}
		
		// else try to shutdown the connections
		try { 
			this.connectionFac.shutDownConnection();
			this.connectionFac = null;
		} catch (IOException e) {
			LOG.error( "Cannot close the connections or inform the scheduler: " + e.getMessage() , e);
		}
	}
	
	/**
	 * Returns an unmodifiable map of all jobs. If you try to change some entries
	 * you got an exception. You can find a DSCLJob object by its unique ID. This
	 * ID is the Key for this map.
	 * 
	 * @return unmodifiable map of current job objects
	 */
	public Map<String, DSCLJob> getJobList(){
		return Collections.unmodifiableMap( this.jobs );
	}
	
	/**
	 * This method submits a new job and returns this object. You can add no, one or a collection of 
	 * JobStateListener to this job object. The job objects got the current status of this job whether 
	 * you add one or more JobStateListener or not.
	 * 
	 * @param meteorScript to submit
	 * @param stateListener to inform state changes. You also can call this method in this way: {@code createNewJob( <String> );}
	 * @return DSCLJob object of the submitted job
	 */
	public DSCLJob createNewJob( String meteorScript, JobStateListener... stateListener ){
		// create a jobID
		String randomJobID = this.getRandomID();
		
		// create a job object
		DSCLJob job = new DSCLJob( randomJobID, meteorScript );
		
		// add listeners
		for ( JobStateListener listener : stateListener )
			job.addJobStateListener( listener );
		
		// add jobs to internal list
		this.jobs.put( randomJobID, job );
		
		// try to submit
		try { this.connectionFac.submitJob(meteorScript, clientID, randomJobID ); } 
		catch (IOException ioe) { LOG.error( "Cannot submit the job. A traffic problem occured", ioe ); }
		
		// return the job object
		return job;
	}
	
	/**
	 * This method ask the scheduler whether the specified job exists on server side. If it exists
	 * the
	 * 
	 * @param job
	 * @param stateListener
	 */
	public void reconnectJob( DSCLJob job, JobStateListener stateListener ){
		JSONObject requestObject = new JSONObject();	
		
		try {
			requestObject.put( ClientRequests.REQUEST_KEY.toString(), ClientRequests.JOB_KNOWN.toString() );
			requestObject.put( ClientRequests.JOBID_KEY.toString(), job.getID() );
			requestObject.put( ClientRequests.JOBSTATE_KEY.toString(), job.getStatus().toString() );
			
			String corrID = this.getRandomID();
			
			this.connectionFac.sendRequest( requestObject, corrID );
			String reply = this.connectionFac.getReply( corrID, 100);
			
			if ( reply != null && reply.matches("yes") ){
				job.addJobStateListener(stateListener);
				this.jobs.put(job.getID(), job);
			} else {
				job.setStatus( JobStates.INITIALIZE );
			}
		} catch( JSONException jsonE ){
			// TODO
		} catch (ShutdownSignalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Creates a new client object by a given clientID. This clientID is final
	 * and cannot changed while this client is alive.
	 * After you got the client object you have to connect it with
	 * the scheduler service. Call the method connect() to do this.
	 * 
	 * @param clientID final unique identifier
	 * @return the client object (not connected yet)
	 */
	public static DOPAClient createNewClient( final String ID ) {
		return new DOPAClient( ID );
	}
	
	/** TODO - only test area follow - TODO **/
	public static void main( String[] args ){
		DOPAClient client = createNewClient( "TanteEmma" );
		
		System.out.println( "client erstellt" );
		
		client.connect();
		
		System.out.println( "connection erstellt." );
		
	}
}
