package eu.stratosphere.meteor.client.connection;

import java.io.IOException;
import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import eu.stratosphere.meteor.client.DOPAClient;
import eu.stratosphere.meteor.client.DSCLJob;
import eu.stratosphere.meteor.common.MessageBuilder;
import eu.stratosphere.meteor.common.RequestConsumable;

public class LinkConsumer extends QueueingConsumer implements RequestConsumable {

	/**
	 * Unique job object
	 */
	private final DOPAClient client;
	private final String corrID;
	
	public LinkConsumer( DOPAClient client, Channel channel, String corrID ) {
		super(channel);
		this.client = client;
		this.corrID = corrID;
	}
	
	@Override
	public void handleDelivery( String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body )
			throws IOException {
		long deliveryTag = envelope.getDeliveryTag();
		
		if ( !properties.getCorrelationId().matches(corrID) )
			super.getChannel().basicAck(deliveryTag, false);
		
		try {
			Charset charset = Charset.forName( properties.getContentEncoding() );
			JSONObject obj = new JSONObject( new String( body, charset ) );
			
			String jobID = MessageBuilder.getJobID( obj );
			int index = MessageBuilder.getFileIndex( obj );
			String path = MessageBuilder.getLink( obj );
			
			DSCLJob job = client.getJobList().get(jobID);
			
			System.out.println( client.getJobList().containsKey(jobID) );
			System.out.println( jobID );
			System.out.println( client.getJobList() );
			
			job.setResultLink( index, path );
		} catch (JSONException e) {}
		
		super.getChannel().basicAck(deliveryTag, true);
		
		super.getChannel().basicCancel(consumerTag);
	}
}
