package eu.stratosphere.meteor.server;

import java.io.IOException;
import java.util.Date;

import org.json.JSONObject;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import eu.stratosphere.meteor.SchedulerConfigConstants;

public class ServerConnectionFactory {
	
	/**
	 * The exchange name to handle responses for clients.
	 */
	protected static final String STATUS_EXCHANGE = "dopa.scheduler.exchange.status";
	
	/**
	 * The type of exchange for job status
	 */
	protected static final String STATUS_EXCHANGE_TYPE = "direct";
	
	/**
	 * The status queues doesn't survive server restarts. If the server restart
	 * all clients have to register for the service again.
	 */
	protected static boolean STATUS_EXCHANGE_DURABLE = false;
	
	/**
	 * There are two kinds of routing keys. A key for job-submissions and one for requests.
	 * Job: 'setJob.*.*.#'
	 * Request: 'requestStatus.*.*'
	 * HandShake: 'handShake.*'
	 */
	private final String[] keys = new String[]{
			SchedulerConfigConstants.JOB_KEY_MASK,
			SchedulerConfigConstants.REQUEST_KEY_MASK,
			SchedulerConfigConstants.REGISTER_KEY_MASK
	};
	
	/**
	 * The queue for incoming messages.
	 */
	private final String REQUEST_QUEUE_NAME = "scheduler.requests.queue";
	
	/**
	 * Standard charset for the scheduler.
	 */
	private final String charset = "UTF-8";
	
	/**
	 * Connection objects
	 */
	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Channel requestChannel, responseChannel;
	private QueueingConsumer consumer;
	
	/**
	 * This default constructor initialize all connections and queues for the complete server side.
	 * To change settings for this you have to change the constants in {@link SchedulerConfigConstants}.
	 */
	protected ServerConnectionFactory(){
		System.out.print( "[X Scheduler] Initialize connections to RabbitMQ" );
		
		// create connection connectionFactory
		this.connectionFactory = new ConnectionFactory();
		
		// the rabbitMQ service already runs on this system (the server system)
		this.connectionFactory.setHost( "localhost" );
		
		try {
			System.out.print( "." );
			
			// creates a requestChannel to a connection
			this.connection = connectionFactory.newConnection();
			this.requestChannel = connection.createChannel();
			this.responseChannel = connection.createChannel();
			
			System.out.print( "." );
			
			// connect the exchange and the queue
			this.declareRequestExchange();
			
			System.out.print( "." );
			
			// connect the exchange and the queue
			this.declareResponseExchange();
			
			System.out.print( "." );
			
			// creates a consumer to receive messages asynchronously
			this.consumer = new QueueingConsumer( requestChannel );
			this.requestChannel.basicConsume( 
					REQUEST_QUEUE_NAME, 
					SchedulerConfigConstants.REQUEST_AUTO_ACKNOWLEDGES, 
					consumer 
					);
			
			System.out.println( "." );
		} catch (IOException e) {
			System.err.println("[X Scheduler] Cannot connect to RabbitMQ... Scheduler stoped.");
			e.printStackTrace();
			return;
		}
		
		System.out.println( "[X Scheduler] Done. Scheduler is ready to use." );
	}
	
	/**
	 * Connect the scheduler with the requests/job queue and binds it with the
	 * exchange queue for requests/jobs. So if a request/job was submitted the scheduler received
	 * only the requests/jobs and not other messages.
	 * 
	 * This exchange is durable by default (see {@link SchedulerConfigConstants}).
	 * 
	 * @throws IOException if an error is encountered
	 */
	private void declareRequestExchange() throws IOException{
		// exchange declaration
		this.requestChannel.exchangeDeclare( 
				SchedulerConfigConstants.REQUEST_EXCHANGE,
				SchedulerConfigConstants.REQUEST_EXCHANGE_TYPE,
				SchedulerConfigConstants.REQUEST_EXCHANGE_DURABLE
		);
		
		// declare the queue for requests/jobs
		this.requestChannel.queueDeclare(
				REQUEST_QUEUE_NAME, 
				SchedulerConfigConstants.REQUEST_EXCHANGE_DURABLE, 
				true, // exclusive for this connection
				false, // not an auto-deleted queue
				null // no other arguments
				);
		
		/**
		 * Binding exchange with the request queue.
		 * Only the messages with correct routingKey may saved in queue
		 * other messages will be lost.
		 */
		for(String bindingKey : keys)
			requestChannel.queueBind (
					REQUEST_QUEUE_NAME, 
					SchedulerConfigConstants.REQUEST_EXCHANGE, 
					bindingKey
					);
	}
	
	/**
	 * Connect the second channel with an exchange for status. This exchange
	 * push job status to an individual queue per client.
	 * 
	 * This exchange is always not durable and collapse with the server.
	 * 
	 * This exchange will not bind to queues by default cause there are no
	 * clients to listen by default.
	 * 
	 * @throws IOException if an error encountered
	 */
	private void declareResponseExchange() throws IOException{
		this.responseChannel.exchangeDeclare(
				STATUS_EXCHANGE,
				STATUS_EXCHANGE_TYPE,
				STATUS_EXCHANGE_DURABLE
				);
	}
	
	/**
	 * If a new client wants to register the service the ConnectionFactory adds new status queue
	 * to the exchange. Or if the client wants to unsubscribes the service its deleted the queue.
	 * 
	 * @param handShakeType can be 'login' or 'logoff'
	 */
	private void register( QueueingConsumer.Delivery delivery ) throws IOException {		
		String replyQueue = delivery.getProperties().getReplyTo();
		String encoding = delivery.getProperties().getContentEncoding();
		
		//boolean isAllowed = DOPAScheduler.addClient( new String( delivery.getBody(), encoding ) );
		
		// TODO if false the client needs to know it
		
		this.requestChannel.basicPublish(
				"", 
				replyQueue, 
				delivery.getProperties(), 
				STATUS_EXCHANGE.getBytes( encoding )
				);
	}
	
	/**
	 * Sends a status of a job to a status queue of the client. This status could be a
	 * JSON string.
	 * 
	 * @param clientName to send the message to the correct client
	 * @param status of job
	 * @throws IOException if an error encountered
	 */
	protected void sendJobStatus( String clientName, JSONObject status ) throws IOException {
		// build properties for contentType and time stamp
		BasicProperties props = new BasicProperties
				.Builder()
				.contentType("application/json")
				.contentEncoding( charset )
				.timestamp( new Date() ) // default constructor represents 'NOW'
				.build();
		
		// finally send the message
		this.responseChannel.basicPublish(
	    		STATUS_EXCHANGE, 
	    		SchedulerConfigConstants.getRoutingKey(clientName), 
	    		props,
	    		status.toString().getBytes( charset )
	    		);
		
		System.err.println( "Send status with routingKey: " + SchedulerConfigConstants.getRoutingKey(clientName) );
	}
	
	/**
	 * Reply a request. To do this we need the original properties from the request and get the
	 * name of the temporary reply queue and the correlation ID for this order. After the properties
	 * set we sends the reply with informations about the content type (application/json for instance).
	 * If no content type is set its use text/plain by default.
	 * 
	 * @param requestProperties original properties from the request
	 * @param encoding of reply
	 * @param answer the reply itself
	 * @throws IllegalArgumentException if any parameter doesn't set correct
	 * @throws IOException cannot send the reply
	 */
	protected void replyRequest( BasicProperties requestProperties, String encoding, String answer ) 
			throws IllegalArgumentException, IOException {
		// get reply_to queue name and correlationID for this request
		String reply_To = requestProperties.getReplyTo();
		String corrID = requestProperties.getCorrelationId();
		
		// if there is no informations about the queue throw an exception
		if ( reply_To == null || encoding == null ) 
			throw new IllegalArgumentException(
					"One of the parameters aren't correct. Be sure you use the original properties from the request."
					);
		
		// build reply properties
		BasicProperties replyProps = new BasicProperties
				.Builder()
				.contentEncoding( encoding )
				.correlationId( corrID )
				.build();
		
		// else try to reply
		this.requestChannel.basicPublish( "", reply_To, replyProps, answer.getBytes( encoding ) );
	}
	
	/**
	 * Returns a new delivery from the request queue. If there are no new deliveries
	 * in {@value timeOut} milliseconds it returns null.
	 * 
	 * @param timeOut waiting for a new delivery
	 * @return delivery or null if there are no new
	 */
	protected QueueingConsumer.Delivery getRequest( long timeOut ){
		try {
			// try to get the new delivery
			QueueingConsumer.Delivery delivery = consumer.nextDelivery( timeOut );
			
			// if the request queue is empty
			if ( delivery == null ) return null;
			
			// get the routing key from delivery (not from properties)
			String[] routingKey = delivery.getEnvelope().getRoutingKey().split("\\.");
			
			// no registration
			if ( routingKey[0].matches("register") && routingKey[1].matches("login") ) this.register( delivery );
			
			// handle registration
			//if ( routingKey[1].matches("login") ) this.register( delivery );
			//else DOPAScheduler.deleteClient( new String( delivery.getBody(), delivery.getProperties().getContentEncoding() ) );
			//TODO
			return delivery;
		} catch ( IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException exc ) {
			// any error encountered while waiting
			System.err.println("An error encountered while waiting for new deliveries!");
			exc.printStackTrace();
			// return null
			return null;
		}
	}
	
	/**
	 * Shutdown all connections and bindings
	 * @throws IOException
	 */
	protected void shutdownConnections() throws IOException {
		this.responseChannel.close();
		this.requestChannel.close();
		this.connection.close();
	}
}
