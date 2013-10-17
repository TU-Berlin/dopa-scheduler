package eu.stratosphere.meteor.server.executors;

import org.junit.Test;
import org.mockito.Mockito.*;

public class JobExecuterTests {
	static RRjob c1a=new RRjob("requestStatus","c001","j001");
	static RRjob c1b=new RRjob("requestStatus","c001","j002");
	static RRjob c2a=new RRjob("requestStatus","c002","j1");
	static RRjob c3a=new RRjob("requestStatus","c003","j01");

	@Test
	public void testIrgendwas(){
		System.out.println( "huhu" );
	}

}
