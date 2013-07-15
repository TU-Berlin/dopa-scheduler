package eu.stratosphere.meteor.common;

/**
 * Vorsicht!
 * Dies ist eine erste Testversion. Sie enthält herangehensweisen die
 * später noch überarbeitet werden müssen. Zum beispiel verwenden wir
 * keine Topics für die Queues etc.
 *
 * @author André Greiner-Petter
 *
 */
public interface Submitable {
	
	/**
	 * Creates a rabbitMQ queue to get the results of the scheduler.
	 * This queue contains all states of a submitted job.
	 * 
	 * @param queueName
	 */
	public void createStatusQueue( final String queueName );
	
	/**
	 * Submits a meteor script to scheduler through RabbitMQ.
	 * It creates a correlation identification number (corrID) to
	 * specified this job.
	 * 
	 * @param meteorScript
	 */
	public void submit( String meteorScript );
	
	/**
	 * Try to get the result from the status queue given by a specific
	 * correlation ID.
	 * 
	 * @param corrID
	 * @return status from statusQueue
	 */
	public String getResult( long corrID );
}
