package eu.stratosphere.meteor.client;

import java.io.IOException;
import java.util.UUID;

import com.hp.hpl.jena.graph.Factory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import eu.stratosphere.meteor.common.Submitable;

/**
 * Der client wird mit main(String[] args) gestartet
 * und soll vorerst nur einen einfachen String submitten.
 * Sowas wie "Hallo du da!".
 * 
 * Das Ergebnis wäre dann eine Antwort des Servers in der
 * statusQueue.
 *
 * @author André Greiner-Petter
 *
 */
public class DOPAClient implements Submitable {
	private Connection connection;
	private Channel channel;
	private QueueingConsumer requestQueue;
	private QueueingConsumer jobQueue;
	private String requestQueueName = "rpc_queue";
	private String replyQueueName;   //for reply_to

    private UUID corrIdU = java.util.UUID.randomUUID();
    private long corrIdLong =corrIdU.getLeastSignificantBits();

	@Override
	public void createStatusQueue(String queueName) {
		// TODO Auto-generated method stub
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			replyQueueName = channel.queueDeclare().getQueue(); 
			requestQueue = new QueueingConsumer(channel);
			jobQueue = new QueueingConsumer(channel);
			
			channel.basicConsume("request", true, requestQueue);			
			channel.basicConsume("job", false, jobQueue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void submit(String meteorScript) {
		// TODO Auto-generated method stub
		try {
				String response = null; 
				channel.queueDeclare(requestQueueName, false, false, false, null);
			    String corrId = corrIdU.toString();
		    	 
		    	BasicProperties props = new BasicProperties
		    	                                .Builder()
		    	                                .correlationId(corrId)
		    	                                .replyTo(replyQueueName)
		    	                                .build();
	
		    	channel.basicPublish("", requestQueueName, props, meteorScript.getBytes());
	
		    	    while (true) {
		    	        QueueingConsumer.Delivery delivery = requestQueue.nextDelivery();
		    	        if (delivery.getProperties().getCorrelationId().equals(corrId)) {
		    	            response = new String(delivery.getBody());
		    	            break;
		    	        }
		    	    }
		    	channel.close();
				connection.close();
		 } catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		    System.out.println(" [x] Sent '" + meteorScript + "'");
	}

	@Override
	public String getResult(long corrID) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args)throws java.io.IOException {
		// TODO Auto-generated method stub
		DOPAClient acall = new DOPAClient();

		System.out.println(" [x] Requesting fib(30)");   
		acall.createStatusQueue("hihi");
		acall.submit("Hallo du da!");
	
	}

}
