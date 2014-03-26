package eu.stratosphere.meteor.server.executor;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.server.executor.JobExecutor;
import eu.stratosphere.meteor.server.executor.RRJob;

/**
 * 
 *
 * @author André Greiner-Petter
 *
 */
public class JobExecuterTest {
	/**
	 * The time threshold for tests in milliseconds
	 */
	private static final long THRESHOLD = 10_000;
	
	/**
	 * The test script should work fine on local machine with test.json in /dopa-vm/
	 */
	private static final String script = 
			"$students = read from '/test.json';" +
			"write $students to '/test_result.json';";
	
	/**
	 * Creates test job objects.
	 */
	private static final Date now = new Date();
	private static RRJob jobFailure = new RRJob("c001", "j001", "ProvokeError", now );
	private static RRJob jobClean = new RRJob("c002", "j002", script, now);
	
	/**
	 * 
	 */
	@Test ( timeout = THRESHOLD )
	public void submitFailureTest(){
		// we don't want to execute this test parallel so we invoke "run" instead of "start"
		new JobExecutor( jobFailure ).run();
		assertEquals ( JobState.ERROR, jobFailure.getStatus() );
	}
	
	@Test ( timeout = THRESHOLD )
	@Ignore ( "Just an integration test." )
	public void submitTest(){
		new JobExecutor( jobClean ).run();
		assertEquals ( "The job failed with information: " + jobClean.getErrorJSON(), JobState.FINISHED, jobClean.getStatus() );
	}
}
