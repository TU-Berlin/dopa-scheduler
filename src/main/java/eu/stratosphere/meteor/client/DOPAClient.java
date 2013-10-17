package eu.stratosphere.meteor.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

import eu.stratosphere.meteor.common.DSCLJob;
import eu.stratosphere.meteor.common.JobStateListener;

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
	private static final Log LOG = LogFactory.getLog( DOPAClient.class );
	
	/**
	 * The unique clientID of this client object
	 */
	private final String clientID;
	
	/**
	 * The connection factory to handle all traffic from and to this client
	 */
	private ClientConnectionFactory connectionFac;
	
	private ArrayList<DSCLJob> jobList;
	
	/**
	 * Constructs a new client object. This client isn't connected
	 * to scheduler yet.
	 * @param clientID final unique identifier
	 */
	private DOPAClient( final String ID ) {
		this.clientID = ID;
		this.connectionFac = null;
		this.jobList = new ArrayList<DSCLJob>();
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
		try { this.connectionFac = new ClientConnectionFactory( this.clientID ); }
		catch (Exception exc) { LOG.error( "Cannot connected to the scheduler services: " + exc.getMessage(), exc ); }
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
	 * Returns an unmodifiable list of all jobs.
	 * @return list of current job objects
	 */
	public List<DSCLJob> getJobList(){
		return Collections.unmodifiableList( this.jobList );
	}
	
	/**
	 * 
	 * @param script
	 * @param stateListener
	 * @return
	 */
	public DSCLJob createNewJob( String script, JobStateListener stateListener ){
		// TODO
		return null;
	}
	
	/**
	 * 
	 * @param job
	 * @param stateListener
	 */
	public void reconnectJob( DSCLJob job, JobStateListener stateListener ){
		// TODO
	}
	
	/**
	 * Creates a new client object by a given clientID. This clientID is final
	 * and cannot change while this client is alive.
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
	
	//public void send(  )
	
	public static void main( String[] args ){
		DOPAClient client = createNewClient( "TanteEmma" );
		
		System.out.println( "client erstellt" );
		
		client.connect();
		
		System.out.println( "connection erstellt." );
	}
}
