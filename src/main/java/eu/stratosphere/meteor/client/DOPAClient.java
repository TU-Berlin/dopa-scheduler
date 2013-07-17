package eu.stratosphere.meteor.client;

import java.io.IOException;

import com.hp.hpl.jena.graph.Factory;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

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
	private String QUEUE_NAME;
  
	private Connection connection;
	private Channel statusQueue = null; 
	
    
	@Override
	public void createStatusQueue(String queueName) {
		// TODO Auto-generated method stub
		this.QUEUE_NAME=queueName;
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	  		
	    try {
			connection = factory.newConnection();
		    statusQueue = connection.createChannel();
		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  
	}

	@Override
	public void submit(String meteorScript) {
		// TODO Auto-generated method stub
		
		 try {
			 statusQueue.queueDeclare(QUEUE_NAME, false, false, false, null);
	    	 statusQueue.basicPublish("", QUEUE_NAME, null, meteorScript.getBytes());
			 statusQueue.close();
			 connection.close();
		 } catch (IOException e) {
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
		 String message = "Hello du da!";
		 DOPAClient firstCall=new DOPAClient();
		 firstCall.createStatusQueue("Status");
		 firstCall.submit(message);
	}

}
