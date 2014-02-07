package eu.stratosphere.meteor.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import eu.stratosphere.meteor.common.DSCLJob;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.common.JobStateListener;
import eu.stratosphere.meteor.common.SchedulerConfigConstants;

import org.junit.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * For these tests, it's required to got a running DOPAScheduler system 
 * or each test will fail.
 *
 * @author André Greiner-Petter
 *
 */
public class DOPAClientTest {
	/**
	 * The threshold for test reactions on server site
	 */
	private static final int THRESHOLD = 10_000;
	
	/**
	 * Test simple connection to rabbitMQ by defined ports and host address
	 */
	@Test
	public void testRabbitMQConnection(){
		// setup factory with informations
		ConnectionFactory connectFactory = new ConnectionFactory();
		connectFactory.setConnectionTimeout( THRESHOLD );
		connectFactory.setHost( SchedulerConfigConstants.SCHEDULER_HOST_ADDRESS );
		connectFactory.setPort( SchedulerConfigConstants.SCHEDULER_PORT );
		Connection connect = null;
		
		// try to setup the connection itself
		try {
			connect = connectFactory.newConnection();
			connect.createChannel();
		} catch (IOException e) {
			fail( "IOException: " + e.getMessage() );
		} finally {
			try { connect.close(); }
			catch ( NullPointerException | IOException e ){};
		}
	}
	
	/**
	 * Test connection setup to scheduler.
	 */
	@Test ( timeout = THRESHOLD )
    public void testConnectDisconnect () {
        DOPAClient client = DOPAClient.createNewClient("connectDisconnectID");
        assertTrue("Could not connect client to scheduler", client.connect() );
        assertTrue( client.isConnected() );
        client.disconnect();
        assertFalse( client.isConnected() );
    }
	
	/**
	 * Test connection by multiple clients
	 */
	@Test ( timeout = THRESHOLD )
	public void testMultipleConnections () {
		DOPAClient client1 = DOPAClient.createNewClient("multipleClient1");
		assertTrue( "Could not connect client to scheduler.", client1.connect() );
		DOPAClient client2 = DOPAClient.createNewClient("multipleClient2");
		assertTrue("Coueld not connect second client to scheduler.", client2.connect() );
		assertTrue(client2.isConnected());
		client1.disconnect();
		client2.disconnect();
	}
	
	/**
	 * Test reconnection setup to scheduler.
	 */
    @Test ( timeout = THRESHOLD )
    public void testReconnect () {
        // connect first client
        DOPAClient client = DOPAClient.createNewClient("testReconnectID");
        assertTrue("First client could not connect to scheduler", client.connect());
        
        // leave connection open
        client = null;

        // connect second client with same ID
        DOPAClient client2 = DOPAClient.createNewClient("testReconnectID");
        
        // connection should fail as the first client didn't disconnect properly
        assertFalse("Second client did connect to server, connection was expected to fail!", client2.connect());
        assertFalse( client2.isConnected() );
        
        // explicit reconnect should work
        assertTrue("Reconnecting second client failed", client2.reconnect());
        
        // do proper disconnect
        client2.disconnect();
    }

    /**
     * High level test. Test to submit a query to the scheduler system.
     */
    @Test ( timeout = THRESHOLD )
    public void testQuerySubmissionError () {
    	// create test client
        DOPAClient client = DOPAClient.createNewClient("testID");
        assertTrue("Could not connect client to scheduler", client.connect());
        
        // Create a default listener
        JobStateListener listener = new JobStateListener() {
            @Override
            public void stateChanged(DSCLJob job, JobState newStatus) {
                System.out.println("Changed JobState " + newStatus.toString());
            }
        };

        // create a job object which provokes a syntax error on server site
        DSCLJob job = client.createNewJob("PROVOKE SYNTAX ERROR", listener);
        
        // this test failed automatically after <THRESHOLD> ms
        while ( true ) {
            if ( job.getStatus().equals(JobState.ERROR) ) break;
            
            try { Thread.sleep(500); } 
            catch (InterruptedException e) { fail("Interrupted thread while waiting on status update."); }
        }
        
        client.disconnect();
        // reach this line means the test finished well
    }
    
    @Test
    public void testQueryWaiting () {
        DOPAClient client = DOPAClient.createNewClient("testIDSucess");
        assertTrue("Could not connect client to scheduler", client.connect());
        JobStateListener listener = new JobStateListener() {
            @Override
            public void stateChanged(DSCLJob job, JobState newStatus) {
                System.out.println("Changed JobState " + newStatus.toString());
            }
        };

        DSCLJob job = client.createNewJob("$students = read from 'file:///dopa-vm/test.json';"
        		+"write $students to 'file:///dopa-vm/test_result.json';", listener);
        
        long start = System.currentTimeMillis();
        boolean expectedFlag = false;
        
        // wait for 10.000ms = 10 seconds
        while ( System.currentTimeMillis() - start < 10_000 ) {
            if ( job.getStatus().equals(JobState.WAITING) ) {
               expectedFlag = true;
            }
            try {Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.disconnect();
        assertTrue("Job doesn't finished with an error in 10 seconds.", expectedFlag);
    }
	
}
