package eu.stratosphere.meteor.server;

import java.io.IOException;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * 
 * Represents the server to submit jobs.
 * 
 * It submits each job and put the results into a
 * given statusQueue.
 *
 * @author André Greiner-Petter
 *
 */
public class DOPAScheduler {

	/**
	 * @param args
	 */
	 
	private final static String QUEUE_NAME = "hello from the server";
	
	public void run(){
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Channel channel = null;
		QueueingConsumer consumer = null;
		
		try {
			Connection connection = factory.newConnection();
			channel = connection.createChannel();
			
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(QUEUE_NAME, true, consumer);
			
		    while (true) {
		      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
		      String message = new String(delivery.getBody());
		      System.out.println(" [x] Received '" + message + "'");
		    }
		    
		} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Waiting for messages.");
		
	}
	
	public static void main(String[] args) throws java.io.IOException,
                   java.lang.InterruptedException {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

	}

}
