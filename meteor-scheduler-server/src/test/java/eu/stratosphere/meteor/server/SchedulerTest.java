package eu.stratosphere.meteor.server;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This class tests the DOPAScheduler class. It's not a primary target to test
 * the jobs itself (these things are tested by the JobExecutorTest.java) but to
 * test how the scheduler works with multiple clients, multiple jobs and round
 * robin solutions.
 *
 * @author André Greiner-Petter
 *
 */
public class SchedulerTest {
	/**
	 * The DOPAScheduler
	 */
	private static final DOPAScheduler scheduler = DOPAScheduler.createNewSchedulerSystem();
	
	/**
	 * 
	 */
	@BeforeClass
	@Ignore
	public static void start(){
		scheduler.start();
	}
	
	@AfterClass
	@Ignore
	public static void stop(){
		try { scheduler.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	@Ignore
	public void multipleClientTest(){
		scheduler.addClient("ben");
	}
}
