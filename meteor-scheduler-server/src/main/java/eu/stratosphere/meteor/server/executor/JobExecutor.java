package eu.stratosphere.meteor.server.executor;

import java.io.IOException;

import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.meteor.common.JobState;
import eu.stratosphere.meteor.server.DOPAScheduler;
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
	
	/**
	 * 
	 * @param job
	 */
	protected JobExecutor( RRJob job ) {
		this.job = job;
		this.meteorScript = job.getMeteorScript();
		this.client = job.getClientFrontend();
	}
	
	@Override
	public void run() {
		try {
			DOPAScheduler.LOG.info("Execute new job " + job.getJobID());
			client.execute( meteorScript );
			job.setStatus( JobState.FINISHED );
			DOPAScheduler.LOG.info("Finished job " + job.getJobID() );
		} catch (QueryParserException e) {
			job.setErrorMessage( "Cannot parse the meteor script of your job. " + e.toString() );
			job.setStatus( JobState.ERROR );
			DOPAScheduler.LOG.warn("Cannot parse the meteor script. " + job.getJobID(), e);
		} catch (Exception e) {
			job.setErrorMessage( "Cannot execute your job. " + e.toString() );
			job.setStatus( JobState.ERROR );
			DOPAScheduler.LOG.warn("Cannot execute the job " + job.getJobID(), e);
		}
	}
}
