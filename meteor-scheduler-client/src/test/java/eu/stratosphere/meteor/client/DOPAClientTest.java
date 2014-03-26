package eu.stratosphere.meteor.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import eu.stratosphere.meteor.common.DSCLJob;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.common.JobStateListener;
import eu.stratosphere.meteor.common.ResultFileBlock;
import eu.stratosphere.meteor.common.ResultFileHandler;
import eu.stratosphere.meteor.common.SchedulerConfigConstants;

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
    
    /**
     * A correct job query on special systems
     */
    private final static String integrationQuery = 
    		"$students = read from '/test.json';\n"+
    		"write $students to '/test_result.json';";
    
    /**
     * Flags to specify status of submitted job
     */
    private static boolean waited = false;
    private static boolean running = false;
    private static boolean finished = false;
    
    private static int blockIdx = 0;
    private static long totalAmountOfBlocks = 1L;
    
    /**
     * Resets the status flags before each job
     */
    @Before
    public void reset(){
    	// back to default values
        waited = false;
        running = false;
        finished = false;
        
        blockIdx = 0;
        totalAmountOfBlocks = 1L;
    }
    
    /**
     * Test to submit a correct job
     * - it's an integration test -
     */
    //@Ignore
    @Test ( timeout = THRESHOLD )
    public void testQuerySubmision () {
        // create client
    	DOPAClient client = DOPAClient.createNewClient("testIDSucess");
        assertTrue("Could not connect client to scheduler", client.connect());
        
        // implement a listener to observer the status changes
        JobStateListener listener = new JobStateListener() {
            @Override
            public void stateChanged(DSCLJob job, JobState newStatus) {
                if ( newStatus.equals( JobState.WAITING ) ) waited = true;
                else if ( newStatus.equals( JobState.RUNNING ) ) running = true;
                else if ( newStatus.equals( JobState.FINISHED ) ) finished = true;
            }
        };
        
        // submit a new job with integration path
        @SuppressWarnings("unused")
		DSCLJob job = client.createNewJob(integrationQuery, listener);
        
        // wait until this job finished
        while ( !finished ) {
        	try {Thread.sleep(100);}
        	catch(InterruptedException ie){break;}
        }
        
        // if it finished, disconnect and test states
        client.disconnect();
        assertTrue("Client doesn't waited to submit the job!", waited);
        assertTrue("The job doesn't runs on the scheduler.", running);
        assertTrue("The job doesn't finished correctly.", finished);
    }
	
    /**
     * This test submit a correct job on special systems and ask for the
     * result. Whether this job finished in a correct test is tested by 
     * 'testQuerySubmission'!
     * - it's an integration test -
     */
    //@Ignore
    @Test ( timeout = THRESHOLD*2 ) // double timeout
    public void testResultRequest(){
    	// initialize
    	DOPAClient client = DOPAClient.createNewClient("ResultClient");
    	client.connect();
    	
    	JobStateListener stateListener = new JobStateListener() {
			@Override
			public void stateChanged(DSCLJob job, JobState newStatus) {
				if ( newStatus.equals( JobState.FINISHED ) || newStatus.equals( JobState.ERROR) ) 
					finished = true;
			}
    	};
    	
    	// submit the new job
    	DSCLJob job = client.createNewJob(integrationQuery, stateListener);
    	
    	// wait until this job finished
    	while ( !finished ){
    		try { Thread.sleep(100); }
    		catch ( InterruptedException ie ){ fail("Job doesn't reached the status FINISHED. Current thread interrupted."); }
    	}
    	
    	// create a result listener
    	ResultFileHandler resultListner = new ResultFileHandler() {
    		@Override
			public void handleFileBlock(DSCLJob job, ResultFileBlock block) {
				blockIdx = block.getBlockIndex();
				totalAmountOfBlocks = block.getTotalNumberOfBlocks();
				System.out.println(
						"New block (Idx:" + block.getBlockIndex() + "/" + block.getTotalNumberOfBlocks() + 
						") of job " + job.getID() + "incoming:");
				System.out.println(block.getStringRepresentation());
				System.out.println();
			}
    	};
    	
    	job.requestResult(0, 1024, Integer.MAX_VALUE, resultListner);
    	
    	while ( blockIdx < totalAmountOfBlocks-1 ){ // blockIdx starts from 0
    		try { Thread.sleep(100); }
    		catch ( InterruptedException ie ){ fail("Job doesn't reached the end of result file. Current thread interrupted."); }
    	}
    }
}
