package eu.stratosphere.meteor.common;

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
	private final int blockSize;
	private final long numOfAllBlocks;
	private final int informationLength;
	
	/** calculated informations **/
	private String stringRepresentation;
	
	/**
	 * It creates a block object and configure usable informations.
	 * @param block the byte[] itself
	 * @param encoding the encoding type for this object
	 * @param blockIdx the index of this block
	 */
	public ResultFileBlock( byte[] block, String encoding, int blockIdx, int blockSize, long numOfAllBlocks ){
		this.encoding = Charset.forName(encoding);
		this.blockIdx = blockIdx;
		this.blockSize = blockSize;
		this.numOfAllBlocks = numOfAllBlocks;
		this.stringRepresentation = new String( block, this.encoding );
		this.informationLength = block.length;
		
		this.block = new byte[blockSize];
		for ( int i = 0; i < block.length; i++ )
			this.block[i] = block[i];
		
		System.out.println( stringRepresentation );
	}

	/**
	 * @return the block
	 */
	public byte[] getBlock() {
		return block;
	}
	
	/**
	 * The block itself got a static size. First block got the same size as each other.
	 * But the current informations are not reaches the end of the block for the whole time.
	 * This informations returns the index where new informations end at the block.
	 * 
	 * So block[0] to block[informationLength()] contains the bytes of this block.
	 * 
	 * @return index where informations ends
	 */
	public int informationLength(){
		return this.informationLength;
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
