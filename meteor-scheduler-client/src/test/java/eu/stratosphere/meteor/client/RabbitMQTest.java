package eu.stratosphere.meteor.client;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQTest {
	
	private ConnectionFactory factory;
	private Connection connection;
	
	@Test ( timeout = 5_000 )
	public void setupTest(){
		factory = new ConnectionFactory();
		factory.setHost( "localhost" );
		
		try { 
			connection = factory.newConnection();
			connection.createChannel();
		} 
		catch (IOException e) {
			fail("Cannot build up connection to rabbit mq");
		}
	}
	
}
