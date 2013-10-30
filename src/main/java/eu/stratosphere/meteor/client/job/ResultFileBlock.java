package eu.stratosphere.meteor.client.job;

import java.nio.charset.Charset;

/**
 * Represents a file block of big file. Created by incoming messages from the scheduler
 * and used by result file handlers.
 *
 * @author André Greiner-Petter
 *
 */
public class ResultFileBlock {
	
	/** final inputs **/
	private final byte[] block;
	private final Charset encoding;
	private final int blockIdx;
	private final long numOfAllBlocks;
	
	/** calculated informations **/
	private int blockSize;
	private String stringRepresentation;
	
	/**
	 * It creates a block object and configure usable informations.
	 * @param block the byte[] itself
	 * @param encoding the encoding type for this object
	 * @param blockIdx the index of this block
	 */
	public ResultFileBlock( byte[] block, String encoding, int blockIdx, long numOfAllBlocks ){
		this.block = block;
		this.encoding = Charset.forName(encoding);
		this.blockIdx = blockIdx;
		this.numOfAllBlocks = numOfAllBlocks;
		this.blockSize = block.length;
		this.stringRepresentation = new String( block, this.encoding );
	}

	/**
	 * @return the block
	 */
	public byte[] getBlock() {
		return block;
	}

	/**
	 * @return the encoding
	 */
	public Charset getEncodingType() {
		return encoding;
	}

	/**
	 * @return the blockIdx
	 */
	public int getBlockIndex() {
		return blockIdx;
	}

	/**
	 * @return the blockSize
	 */
	public int getBlockSize() {
		return blockSize;
	}
	
	/**
	 * @return the total number of all blocks chosen by scheduler
	 */
	public long getTotalNumberOfBlocks() {
		return numOfAllBlocks;
	}

	/**
	 * @return the stringRepresentation
	 */
	public String getStringRepresentation() {
		return stringRepresentation;
	}
	
	
}
