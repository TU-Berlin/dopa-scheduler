package eu.stratosphere.meteor.client.connection;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import eu.stratosphere.meteor.client.DOPAClient;
import eu.stratosphere.meteor.client.DSCLJob;
import eu.stratosphere.meteor.client.job.ResultFileBlock;
import eu.stratosphere.meteor.client.job.ResultFileHandler;
import eu.stratosphere.meteor.common.MessageBuilder;
import eu.stratosphere.meteor.common.RequestConsumable;

/**
 * Handle incoming file blocks asynchronously. Its an request consumable class so it's
 * implemented the marker interface to show it can handle replies from requests.
 *
 * TODO PROGRESS
 * 1) JSONObject with fileIdx, maxBlockNums
 * 2) byte[] of file (blocks)
 * 3) JSONObject with finished informations (time stamp for instance)
 * 		handle cancel options and delete handlers on job objects after finished reply session
 *
 * @author André Greiner-Petter
 *
 */
public class ResultConsumer extends QueueingConsumer implements RequestConsumable {
	
	/** client object **/
	private final DOPAClient client;
	
	/** correlation ID of request **/
	private final String corrID;
	
	/**
	 * Informations has to be filled by first message
	 */
	private int fileIndex = 0;
	private int blockIdx = 0;
	private long maxBlockNumbers = 0;
	private String jobID = null;
	
	/**
	 * Create a specified consumer to handle incoming result messages asynchronously.
	 * @param client DOPAClient
	 * @param ch channel
	 * @param correlationID of incoming message
	 * TODO security?
	 */
	public ResultConsumer( DOPAClient client, Channel ch, String correlationID ) {
		super(ch);
		this.client = client;
		this.corrID = correlationID;
	}
	
	/**
	 * This method invoked asynchronously each time a new delivery incoming.
	 * It handles first message with informations about the following blocks.
	 */
	@Override
	public void handleDelivery( String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body )
			throws IOException {
		// get delivery tag to acknowledge rabbitMQ
		long deliveryTag = envelope.getDeliveryTag();
		
		// false delivery
		if ( !properties.getCorrelationId().matches(corrID) ){
			// acknowledge rabbitMQ as well
			super.getChannel().basicAck( deliveryTag, false );
		}
		
		// if incoming delivery is the information message
		if ( properties.getContentType().contains("json") ){
			String charSet = properties.getContentEncoding();
			String jsonString = new String( body, charSet );
			
			try {
				// fill following informations
				JSONObject obj = new JSONObject( jsonString );
				maxBlockNumbers = MessageBuilder.getMaxNumOfBlocks( obj );
				jobID = MessageBuilder.getJobID( obj );
				fileIndex = MessageBuilder.getFileIndex( obj );
			} catch (JSONException e) {}
			
			// acknowledge rabbitMQ
			super.getChannel().basicAck( deliveryTag, false );
			return;
		}
		
		// else incoming delivery is a file block.
		try{
			// create block from incoming message
			ResultFileBlock block = new ResultFileBlock( body, properties.getContentEncoding(), blockIdx++, maxBlockNumbers );
			
			// get file handler
			DSCLJob job = client.getJobList().get( jobID );
			ResultFileHandler handler = job.getResultHandler().get( fileIndex );
			
			// inform file handler
			handler.handleFileBlock( job, block );
		} catch ( NullPointerException npe ){
			DOPAClient.LOG.error("Cannot receive file by missing meta informations.", npe);
		}
		
		// acknowledge rabbitMQ as well
		super.getChannel().basicAck( deliveryTag, false );
	}
}
