package eu.stratosphere.meteor.server.executors;

import java.util.Date;

import org.junit.Test;

import eu.stratosphere.meteor.server.executor.RRJob;

public class JobExecuterTests {
	static Date now = new Date();
	static RRJob c1a=new RRJob("requestStatus","c001","j001", now);
	static RRJob c1b=new RRJob("requestStatus","c001","j002", now);
	static RRJob c2a=new RRJob("requestStatus","c002","j1", now);
	static RRJob c3a=new RRJob("requestStatus","c003","löl",now);

	@Test
	public void testIrgendwas(){
		System.out.println( "huhu" );
	}

}
