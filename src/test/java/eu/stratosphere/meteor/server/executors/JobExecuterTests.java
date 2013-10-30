package eu.stratosphere.meteor.server.executors;

import java.util.Date;

import org.junit.Test;
import org.mockito.Mockito.*;

public class JobExecuterTests {
	static Date now = new Date();
	static RRjob c1a=new RRjob("requestStatus","c001","j001", now);
	static RRjob c1b=new RRjob("requestStatus","c001","j002", now);
	static RRjob c2a=new RRjob("requestStatus","c002","j1", now);
	static RRjob c3a=new RRjob("requestStatus","c003","löl",now);

	@Test
	public void testIrgendwas(){
		System.out.println( "huhu" );
	}

}
