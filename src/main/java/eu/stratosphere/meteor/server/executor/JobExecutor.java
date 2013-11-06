package eu.stratosphere.meteor.server.executor;

import java.io.IOException;

import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * Just a beta and not tested yet. Should submit a job parallel!
 *
 * @author André Greiner-Petter
 *
 */
public class JobExecutor extends Thread {
	
	private final RRJob job;
	
	private String meteorScript;
	private ClientFrontend client;
	
	protected JobExecutor( RRJob job ) {
		this.job = job;
		this.meteorScript = job.getMeteorScript();
		this.client = job.getClientFrontend();
	}
	
	@Override
	public void run() {
		try {
			job.setStatus( JobState.RUNNING );
			client.execute( meteorScript );
			job.setStatus( JobState.FINISHED );
			job.setOutputStrings( client.getOutputPaths() );
		} catch (QueryParserException e) {
			job.setErrorMessage( "Cannot parse the meteor script of your job. " + e.getMessage() );
		} catch (IOException e) {
			job.setErrorMessage( "Cannot execute your job. " + e.getMessage() );
		}
	}
}
