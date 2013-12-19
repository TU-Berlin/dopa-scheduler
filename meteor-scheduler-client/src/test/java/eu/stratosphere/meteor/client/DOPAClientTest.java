package eu.stratosphere.meteor.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import eu.stratosphere.meteor.common.DSCLJob;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.common.JobStateListener;

import org.junit.Test;

public class DOPAClientTest {
	
	@Test
    public void testConnectDisconnect () {
        DOPAClient client = DOPAClient.createNewClient("connectDisconnectID");
        assertTrue(client.connect());
        client.disconnect();
    }

    @Test
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
        // explicit reconnect should work
        assertTrue("Reconnecting second client failed", client2.reconnect());
        // do proper disconnect
        client2.disconnect();
    }


    @Test
    public void testQuerySubmission () {
        DOPAClient client = DOPAClient.createNewClient("testID");
        assertTrue("Could not connect client to scheduler", client.connect());
        JobStateListener listener = new JobStateListener() {
            @Override
            public void stateChanged(DSCLJob job, JobState newStatus) {
                System.out.println("Changed JobState " + newStatus.toString());
            }
        };

        DSCLJob job = client.createNewJob("PROVOKE SYNTAX ERROR", listener);
        
        long start = System.currentTimeMillis();
        boolean expectedErrorFlag = false;
        
        // wait for 10.000ms = 10 seconds
        while ( System.currentTimeMillis() - start < 10_000 ) {
            if ( job.getStatus().equals(JobState.ERROR) ) {
               expectedErrorFlag = true;
            }
            try {Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.disconnect();
        assertTrue("Job doesn't finished with an error in 10 seconds.", expectedErrorFlag);
    }
	
}
