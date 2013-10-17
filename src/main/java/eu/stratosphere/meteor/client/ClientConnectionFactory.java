package eu.stratosphere.meteor.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;

import eu.stratosphere.meteor.SchedulerConfigConstants;
import eu.stratosphere.meteor.common.DSCLJob;
import eu.stratosphere.meteor.common.JobStateListener;

/**
 * This class sends requests and jobs to the server. It handle all connections
 * to rabbitMQ and the scheduler services.
 *
 * @author André Greiner-Petter
 */
public class ClientConnectionFactory {
	
	/** Unique informations about the client **/
	private final String clientID;
	private final String charset = "UTF-8";
	private String statusQueue;
	private String replyQueue;
	
	/** connection informations **/
	private ConnectionFactory connectFactory;
	private Connection connection;
	private Channel requestChannel, statusChannel;
	private QueueingConsumer staticStatusConsumer;
	private QueueingConsumer tmpRequestConsumer;
	
	/** static list of clients who listen on status queue **/
	private List<JobStateListener> listeners = new ArrayList<JobStateListener>();
	
	/**
	 * Create new channel to connect this client with rabbitMQ and the DOPAScheduler.
	 * It creates a channel to send request and another channel to get status.
	 * 
	 * This constructor catch all exception in the initialization process and throw
	 * a general exception with detailed informations.
	 * 
	 * @throws Exception if the factory cannot initialize connections to rabbitMQ
	 */
	protected ClientConnectionFactory( final String clientID ) throws Exception {		
		this.clientID = clientID;
		
		// build a connection
		connectFactory = new ConnectionFactory();
		connectFactory.setHost( SchedulerConfigConstants.SCHEDULER_HOST_ADDRESS );
		connectFactory.setPort( SchedulerConfigConstants.SCHEDULER_PORT );
		
		try {
			// create channels
			this.connection = connectFactory.newConnection();
			this.requestChannel = connection.createChannel();
			this.statusChannel = connection.createChannel();
			
			// create a non-durable, exclusive, autodelete queue with generated name
			this.statusQueue = this.statusChannel.queueDeclare().getQueue();
			
			// subscribe status queue
			this.subscribe();
			
			// consume the status queue
			this.staticStatusConsumer = new StatusConsumer( statusChannel );
			this.statusChannel.basicConsume( statusQueue, true, staticStatusConsumer );
		} catch ( ShutdownSignalException 
				| ConsumerCancelledException
				| InterruptedException e ) {
			// handshake failed
			Exception exc = new Exception( "The handshake with the DOPAScheduler failed." );
			exc.setStackTrace( e.getStackTrace() );
			throw exc;
		} catch ( IOException ioe ) {
			// any other failed
			Exception exc = new Exception( "IOException encountered..." );
			exc.setStackTrace( ioe.getStackTrace() );
			throw exc;
		}
	}
	
	/**
	 * Subscribes the status queue. This clients need to authenticate itself at the service and get the
	 * name of exchange for job status. If we get this exchange name we bind our status queue with this 
	 * exchange with an generatedKey for routingKey.
	 * 
	 * @throws IOException if an error encountered
	 * @throws InterruptedException if the connection interrupted through the handshake
	 * @throws ConsumerCancelledException if the staticStatusConsumer cancelled while waiting for an answer
	 * @throws ShutdownSignalException if rabbitMQ shutdown through handshake
	 */
	private void subscribe() 
			throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException{
		// initialize handshake components
		String handShakeQueue = this.requestChannel.queueDeclare().getQueue();
		QueueingConsumer handShakeConsumer = new QueueingConsumer( this.requestChannel );
		this.requestChannel.basicConsume(handShakeQueue, true, "handShakeConsumer", handShakeConsumer);
		
		// create properties for correct encoding and reply
		BasicProperties props = new BasicProperties.
				Builder().
				replyTo( handShakeQueue ).
				contentEncoding( charset ).
				build();
		
		// register on server to get the correct exchange
		this.requestChannel.basicPublish(
				SchedulerConfigConstants.REQUEST_EXCHANGE, 
				"register.login", 
				props, 
				clientID.getBytes(charset) ); // TODO name or authentification informations
		
		// wait for the name of status exchange to bind our status queue to this exchange
		QueueingConsumer.Delivery delivery = handShakeConsumer.nextDelivery();
		String status_exchange = new String( delivery.getBody(), charset );
		
		// bind the queue with the exchange
		this.statusChannel.queueBind( this.statusQueue, status_exchange, SchedulerConfigConstants.getRoutingKey(clientID) );
		
		// close and delete all handshake components
		this.requestChannel.basicCancel( "handShakeConsumer" );
	}
	
	/**
	 * Unsubscribes the service and inform the scheduler.
	 * @throws IOException if cannot inform the scheduler
	 */
	private void unsubscribe() throws IOException {
		// create properties for correct encoding
		BasicProperties props = new BasicProperties.
				Builder().
				contentEncoding( charset ).
				build();
		
		// inform the scheduler
		this.requestChannel.basicPublish (
				SchedulerConfigConstants.REQUEST_EXCHANGE, 
				"register.logoff", 
				props, 
				statusQueue.getBytes( charset ) );
		
		// delete queue
		this.statusChannel.queueDelete( statusQueue );
	}
	
	/**
	 * Returns the status object for a job. If there are no status currently available it
	 * returns null and send a request to the scheduler to get informations about the status.
	 * 
	 * If it returns null try to get the status later.
	 * 
	 * @param timeOut to wait for message
	 * @return json object of status message or null
	 * @throws ShutdownSignalException if connection is shutdown while waiting
	 * @throws ConsumerCancelledException if the staticStatusConsumer is cancelled while waiting
	 * @throws InterruptedException if an interrupt is received while waiting
	 * @throws UnsupportedEncodingException if there are no correct encoding informations
	 * @throws JSONException if we cannot rebuild a json object from message
	 * @throws IOException if we cannot send the request
	 */
	protected JSONObject getStatus( long timeOut ) 
			throws ShutdownSignalException, ConsumerCancelledException, InterruptedException, 
			UnsupportedEncodingException, JSONException, IOException 
			{
		// TODO ist die nachricht aktuell
		// get message in timeOut milliseconds
		QueueingConsumer.Delivery delivery = staticStatusConsumer.nextDelivery( timeOut );
		
		// if there is a new status return status
		if ( delivery != null ){
			// if status message is not a json string
			if ( !delivery.getProperties().getContentType().contains("json") )
				throw new JSONException( "Expected json status but was another object type: " + delivery.getProperties().getContentType() );		
			
			String charSet = delivery.getProperties().getContentEncoding();
			String jsonString = new String( delivery.getBody(), charSet );
			
			return new JSONObject( jsonString );
		}
		
		sendRequest( "status", "1" );
		return null;
	}

	/**
	 * Send a job to the scheduler with encoding informations and a time stamp. If the scheduler try to
	 * submit this job 'too late' the scheduler can ask the client before submits his job.
	 * 
	 * @param meteorScript the meteor script represents the job
	 * @param jobType type of this job
	 * @param clientID of this client
	 * @param jobID to specify this job
	 * @throws IOException
	 */
	public void submitJob( String meteorScript, String jobType, String clientID, String jobID ) throws IOException {
		BasicProperties jobProps = new BasicProperties
				.Builder()
				.contentEncoding(charset)
				.timestamp( new Date() )
				.build();
		
		requestChannel.basicPublish(
	    		SchedulerConfigConstants.REQUEST_EXCHANGE, 
	    		"setJob." + jobType + "." + clientID + "." + jobID, 
	    		jobProps,
	    		meteorScript.getBytes( charset )
	    		);
	}
	
	/**
	 * Send a request to the scheduler.
	 * 
	 * @param request
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws ConsumerCancelledException 
	 * @throws ShutdownSignalException 
	 */
	public void sendRequest( String request, String correlationID ) throws IOException, 
			ShutdownSignalException, ConsumerCancelledException, InterruptedException
			{
		// if there is an old staticStatusConsumer waiting for replies
		if ( this.tmpRequestConsumer != null ){
			this.requestChannel.basicCancel( "replyConsumer" );
			System.out.println( "Deleted old reply staticStatusConsumer" );
		}
		
		// random queue for reply
		this.replyQueue = this.requestChannel.queueDeclare().getQueue();
		
		// build properties
		BasicProperties replyProps = new BasicProperties
				.Builder()
				.correlationId( correlationID )
				.replyTo( replyQueue )
				.contentEncoding( charset )
				.build();
		
		// consume reply queue
		this.tmpRequestConsumer = new QueueingConsumer( requestChannel );
		this.requestChannel.basicConsume(replyQueue, false, "replyConsumer", tmpRequestConsumer);
		
		// send request
		requestChannel.basicPublish(
				SchedulerConfigConstants.REQUEST_EXCHANGE, 
				"requestStatus.*.*", // TODO which informations are needed in routingKey?
				replyProps,
				request.getBytes()
				);
	}
	
	/**
	 * Returns the reply message from our request. It returns null if there are no replies yet.
	 * It also try to reload another reply if this message doesn't matches with given correlationId.
	 * 
	 * @param correlationID to get the correct reply
	 * @param timeOut waiting time for reply in milliseconds
	 * @return message of reply or null if there are no replies yet
	 * @throws ConsumerCancelledException if this staticStatusConsumer doesn't exist anymore
	 * @throws IOException if cannot cancel the tmpRequestConsumer
	 */
	public String getReply( String correlationID, long timeOut ) throws ConsumerCancelledException, IOException{
		try {
			// get reply from reply queue
			QueueingConsumer.Delivery reply = tmpRequestConsumer.nextDelivery( timeOut );
			
			// if there is no reply yet
			if ( reply == null ) return null;
			
			// if this message is not the correct reply version
			if ( correlationID.matches( reply.getProperties().getCorrelationId() ) ) 
				return getReply( correlationID, timeOut );
			
			// get message
			String replyMessage = new String( reply.getBody(), reply.getProperties().getContentEncoding() );
			
			// clean connections
			this.requestChannel.basicCancel("replyConsumer");
			this.tmpRequestConsumer = null;
			
			return replyMessage;
		} catch (ShutdownSignalException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return null;
	}
	
	protected boolean addListener( JobStateListener stateListener ){
		return this.listeners.add( stateListener );
	}
	
	protected boolean removeListener( JobStateListener stateListener ){
		return this.listeners.remove( stateListener );
	}
	
	private void callListener( DSCLJob job, DSCLJob.State newState ){
		for ( JobStateListener listener : this.listeners )
			listener.stateChanged( job, newState );
	}
	
	/**
	 * Close the connections to the server queues.
	 * @throws IOException cannot close the connections
	 */
	public void shutDownConnection() throws IOException{
		this.unsubscribe();
		this.requestChannel.close();
		this.connection.close();
	}
	
	/**
	 * 
	 *
	 * @author André Greiner-Petter
	 *
	 */
	private class StatusConsumer extends QueueingConsumer {
		
		public StatusConsumer( Channel ch ) {
			super(ch);
		}
		
		@Override
		public void handleDelivery( String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body )
				throws IOException {
			super.handleDelivery(consumerTag, envelope, properties, body);
			
			try {
				JSONObject status = getStatus( 0 );
				
				System.out.println( status );
				
				String jobID = status.getString("JobID");
				String newState = status.getString("State");
				
				System.out.println( "JobID: " + jobID + ", newState: " + DSCLJob.State.valueOf(newState) );
				
				callListener( null, null );
			} catch (ShutdownSignalException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ConsumerCancelledException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}
}
