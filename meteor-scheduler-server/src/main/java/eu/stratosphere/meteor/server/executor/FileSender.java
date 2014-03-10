package eu.stratosphere.meteor.server.executor;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import eu.stratosphere.meteor.common.SchedulerConfigConstants;
import eu.stratosphere.meteor.common.MessageBuilder;
import eu.stratosphere.meteor.server.DOPAScheduler;
import eu.stratosphere.meteor.server.ServerConnectionFactory;

/**
 * This class sends a file (from local file system or hadoop) back to the client.
 * The FileSender separates the file to blocks and send each block back to the client.
 *
 * @author André Greiner-Petter
 *
 */
public class FileSender extends Thread {
	/**
	 * The default encoding type of hadoop. We don't change that.
	 */
	public static final Charset HDFS_CHARSET = Charset.forName("UTF-8");
	
	/** 
	 * split full hdfs path into host and path component. 
	 * first group is host, second is path
	 */
	private static final Pattern hdfsPattern = Pattern.compile("(hdfs://[a-zA-Z0-9\\.:\\-]+)(/[a-zA-Z\\./_]+)");
	
	/**
	 * Pattern to get path to local file system
	 */
	private static final Pattern localPattern = Pattern.compile("file://(/[\\w\\./:\\-_]+)");
	
	/**
	 * High Distributed File System
	 */
	private FileSystem hdfs;
	
	/**
	 * Connection factory to send blocks
	 */
	private ServerConnectionFactory connFac;
	
	/**
	 * Job itself includes paths
	 */
	private RRJob job;
	
	/**
	 * Save request properties to send replies correctly
	 */
	private final BasicProperties requestProps;
	
	private Charset charset;
	
	/**
	 * Block informations
	 */
	private long sumOfBlocks;
	private int blockSize;
	private int fileIndex;
	
	/**
	 * Creates a FileSender object which sends (in a Thread) blocks to the client.
	 * @param connFac the ServerConnectionFactory
	 * @param job
	 * @param delivery original from request
	 */
	public FileSender( ServerConnectionFactory connFac, RRJob job, Delivery delivery ){
		this.job = job;
		this.connFac = connFac;
		this.charset = Charset.forName( delivery.getProperties().getContentEncoding() );
		
		// try to get all informations included in the request
		this.filterDelivery( job, delivery );
		
		// set this charset to HDFS charset
		this.charset = HDFS_CHARSET;
		
		// build copy of properties with new encoding type
		this.requestProps = new BasicProperties
				.Builder()
				.contentType( SchedulerConfigConstants.JSON )
				.contentEncoding( charset.name() )
				.correlationId( delivery.getProperties().getCorrelationId() )
				.replyTo( delivery.getProperties().getReplyTo() )
				.build();
	}
	
	/**
	 * Filter all informations of the given JSON request. Save desired block size and maximum number of blocks over all.
	 * After you parallelized this thread (call run) this informations may be changed because of new informations about
	 * the requested file.
	 * @param delivery request itself
	 */
	private void filterDelivery( RRJob job, Delivery delivery ){
		// get JSON request itself.
		JSONObject request;
		try { request = new JSONObject( new String( delivery.getBody(), charset ) ); }
		catch (JSONException e) { return; } // never reached that catch clause
		
		// save desired block size from client
		int desiredBlockSize = MessageBuilder.getDesiredBlockSize(request);
		
		// if the desired size is quite to big we set to specific maximum
		if ( desiredBlockSize >= SchedulerConfigConstants.MAX_BLOCK_SIZE )
			blockSize = SchedulerConfigConstants.MAX_BLOCK_SIZE;
		else blockSize = desiredBlockSize;
		
		// after all sets the sum of all blocks to default ( it will be changed later )
		sumOfBlocks = MessageBuilder.getMaxNumOfBlocks(request);
		fileIndex = MessageBuilder.getFileIndex(request);
	}
	
	/**
	 * Start parallel thread.
	 */
	@Override
	public void run(){
		// open connection to HDFS
		String host = null;
		String path = null;
		FileStatus fileStatus = null;
		
		String result = job.getMappedResult(fileIndex);
		if ( result == null ) {
			DOPAScheduler.LOG.error("No result found for specified index: " + fileIndex);
			throw new NullPointerException("No result found for specified index: " + fileIndex);
		}
		
		// test whether the link is a local file. In this case we have to use a quite other method
		Matcher matcher = localPattern.matcher( result );
		if ( matcher.find() ){
			this.runLocal( matcher.group(1) );
			return;
		}
		
		// get hdfs paths
		matcher = hdfsPattern.matcher( result );
		if ( matcher.find() ) {
			host = matcher.group(1);
			path = matcher.group(2);
		}
		
		// try to get the file status
		if ( host != null && path != null ){
			try { 
				fileStatus = this.getFileStates(host, path)[0];
				if ( fileStatus == null ) throw new Exception( "No files found at " + path );
			}
			catch ( Exception e ){ 
				DOPAScheduler.LOG.error("Cannot get the file to send results back to the client.", e); 
				throw new IllegalArgumentException(e.getMessage(), e);
			}
		}
		
		// calculate block size or change 
		if ( fileStatus.getLen() / blockSize > sumOfBlocks )
			blockSize = (int) (fileStatus.getLen()/sumOfBlocks);
		else sumOfBlocks = (fileStatus.getLen()+1) / blockSize;
		
		/** Sends informations about following blocks back to the client **/
		
		// first send specifications
		try { this.sendSpecifications(); }
		catch (IllegalArgumentException | IOException e) {
			DOPAScheduler.LOG.error("Cannot send informations about the following blocks to the client.", e);
			throw new IllegalArgumentException("Cannot send informations about the following blocks to the client.", e);
		}
		
		// after send the informations of blocks, send the blocks itself
		try ( FSDataInputStream in = new FSDataInputStream( hdfs.open( fileStatus.getPath() ) ) ) {
			// create a buffer
			byte[] buffer = new byte[blockSize];
			
			// read as long as you can
			int len = in.read( buffer );
			while ( len > 0 ){
				// if we reached the end just send the smaller block, else send complete block
				if ( len < blockSize ) connFac.sendBlock(requestProps, Arrays.copyOfRange(buffer, 0, len));
				else connFac.sendBlock(requestProps, buffer);
				
				len = in.read( buffer );
			} // finally closed channels
		} catch ( IllegalArgumentException iae ){
			DOPAScheduler.LOG.error("Cannot send with this properties.", iae);
			throw new IllegalArgumentException("Cannot send with this properties.", iae);
		} catch ( IOException ioe ){
			DOPAScheduler.LOG.error("Cannod send blocks of result file.", ioe);
			throw new IllegalArgumentException("Cannod send blocks of result file.", ioe);
		}
	}
	
	/**
	 * Handle a local file as a result. Quite different implementation to a file on hadoop.
	 * But the idea is just the same.
	 * @param path to local file
	 */
	private void runLocal( String path ){
		File file = new File( path );
		if ( file.isDirectory() || !file.exists() ){
			DOPAScheduler.LOG.error("Given file is not a directory or doesn't exists: " + path);
			throw new IllegalArgumentException("Given file is not a directory or doesn't exists: " + path);
		}
		
		// calculate block size or change 
		if ( file.length() / blockSize > sumOfBlocks )
			blockSize = (int) ( file.length()/sumOfBlocks);
		else sumOfBlocks = (file.length()+1) / blockSize;
		
		// send specifications
		try { this.sendSpecifications(); }
		catch (IllegalArgumentException | IOException e) {
			DOPAScheduler.LOG.error("Cannot send informations about the following blocks to the client.", e);
			throw new IllegalArgumentException("Cannot send informations about the following blocks to the client.", e);
		}
		
		// open streams, read and send input
		try ( DataInputStream in = new DataInputStream( new FileInputStream( file ) ) ){
			byte[] buffer = new byte[blockSize];
			
			// read from buffer as long as you can
			int len = in.read( buffer );
			while ( len > 0 ){
				// if we reached the end just send the smaller block, else send complete block
				if ( len < blockSize ) connFac.sendBlock(requestProps, Arrays.copyOfRange(buffer, 0, len));
				else connFac.sendBlock(requestProps, buffer);
				
				len = in.read( buffer );
			}
		} catch ( IOException ioe ){
			DOPAScheduler.LOG.error( "Cannot read from local file. A streaming error occurred.", ioe);
			throw new IllegalArgumentException( "Cannot read from local file. A streaming error occurred.", ioe);
		}
	}
	
	/**
	 * Sends the specifications. Should be defined before you send the specifications to the client.
	 * @throws IllegalArgumentException if one or more informations lost
	 * @throws IOException if cannot send the message
	 */
	private void sendSpecifications() throws IllegalArgumentException, IOException{
		// choose definitions and send informations to client
		JSONObject obj = MessageBuilder.buildRequestResult(
				job.getClientID(), 
				job.getJobID(), 
				fileIndex, 
				blockSize,
				sumOfBlocks);
		
		// first send json with informations
		connFac.replyRequest( requestProps, obj );
	}
	
	/**
	 * Returns the file status for all files included on this file path. It can be a directory
	 * path so it returns an array of all status. In general (for this scheduler) its just a file
	 * and returns just one FileStatus object.
	 * @param host of the path
	 * @param filePath complete path to file
	 * @return list of all FileStatus
	 */
	private FileStatus[] getFileStates( String host, String filePath ){
		// get path representation
		Path path = new Path( filePath );
		
		try { // create file system object on hdfs
			Configuration conf = new Configuration();
			this.hdfs = FileSystem.get( new URI(host), conf );
			
			// be sure it exists
			if ( hdfs == null ) return null;
			
			// get file status (one or more files)
			FileStatus[] fileStates;
			
			// if it is a directory list all files, else list this one exactly file
			if ( hdfs.getFileStatus( path ).isDir() ) fileStates = hdfs.listStatus(path);
			else fileStates = new FileStatus[] { hdfs.getFileStatus( path ) };
			
			// return list
			return fileStates;
		} catch (IOException | URISyntaxException e) {
			DOPAScheduler.LOG.error("Cannot handle with the hadoop file system.", e);
			return null;
		}
	}
}
