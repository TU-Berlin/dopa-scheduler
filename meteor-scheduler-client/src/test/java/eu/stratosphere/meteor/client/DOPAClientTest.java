package eu.stratosphere.meteor.client;

import static org.junit.Assert.fail;
import eu.stratosphere.meteor.common.DSCLJob;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.common.JobStateListener;

import org.junit.Test;

public class DOPAClientTest {
	
	@Test
    public void testConnectDisconnect () {
        DOPAClient client = DOPAClient.createNewClient("connectDisconnectID");
        client.connect();
        client.disconnect();
    }


    @Test
    public void testQuerySubmission () {
        DOPAClient client = DOPAClient.createNewClient("testID");
        client.connect();
        JobStateListener listener = new JobStateListener() {
            @Override
            public void stateChanged(DSCLJob job, JobState newStatus) {
                System.out.println("Changed JobState " + newStatus.toString());
            }
        };

        DSCLJob job = client.createNewJob("PROVOKE SYNTAX ERROR", listener);
        
        long start = System.currentTimeMillis();
        
        // wait for 10.000ms = 10 seconds
        while ( System.currentTimeMillis() - start < 10_000 ) {
            if ( job.getStatus().equals(JobState.ERROR) )return;
        	
            try {Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        fail("Job doesn't finished with an error in 10 seconds.");
    }
	
}
