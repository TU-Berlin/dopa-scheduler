package eu.stratosphere.meteor.client;

import eu.stratosphere.meteor.common.Submitable;

/**
 * Der client wird mit main(String[] args) gestartet
 * und soll vorerst nur einen einfachen String submitten.
 * Sowas wie "Hallo du da!".
 * 
 * Das Ergebnis wäre dann eine Antwort des Servers in der
 * statusQueue.
 *
 * @author André Greiner-Petter
 *
 */
public class DOPAClient implements Submitable {

	@Override
	public void createStatusQueue(String queueName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void submit(String meteorScript) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getResult(long corrID) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
