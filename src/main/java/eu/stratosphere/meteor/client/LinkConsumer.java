package eu.stratosphere.meteor.client;

import java.io.IOException;
import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import eu.stratosphere.meteor.common.MessageBuilder;
import eu.stratosphere.meteor.common.RequestConsumable;

/**
 * Consumer handles incoming reply for a link request.
 *
 * @author André Greiner-Petter
 *
 */
public class LinkConsumer extends QueueingConsumer implements RequestConsumable {
	
	/**
	 * Client with collection of all jobs
	 */
	private final DOPAClient client;
	
	/**
	 * Correlation ID for the request
	 */
	private final String corrID;
	
	/**
	 * Creates a QueueingConsumer object
	 * @param client
	 * @param channel
	 * @param corrID
	 */
	protected LinkConsumer( DOPAClient client, Channel channel, String corrID ) {
		super(channel);
		this.client = client;
		this.corrID = corrID;
	}
	
	/**
	 * Handle incoming reply.
	 * @param consumerTag
	 * @param envelope
	 * @param properties
	 * @param body
	 * @throws IOException
	 */
	@Override
	public void handleDelivery( String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body )
			throws IOException {
		// save delivery tag for acknowledgment
		long deliveryTag = envelope.getDeliveryTag();
		
		// if wrong correlation ID
		if ( !properties.getCorrelationId().matches(corrID) ){
			super.getChannel().basicAck(deliveryTag, false);
			return;
		}
		
		try { // try to handle input
			// get object
			Charset charset = Charset.forName( properties.getContentEncoding() );
			JSONObject obj = new JSONObject( new String( body, charset ) );
			
			// get information
			String jobID = MessageBuilder.getJobID( obj );
			int index = MessageBuilder.getFileIndex( obj );
			String path = MessageBuilder.getLink( obj );
			
			// get job
			DSCLJob job = client.getJobList().get(jobID);
			
			// actualize job
			job.setResultLink( index, path );
		} catch (JSONException | NullPointerException e) {
			DOPAClient.LOG.warn("Cannot handle incoming reply for link requests.", e);
		}
		
		// acknowledge rabbitMQ
		super.getChannel().basicAck(deliveryTag, true);
		
		// close this consumer
		super.getChannel().basicCancel(consumerTag);
	}
}
