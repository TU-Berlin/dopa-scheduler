package eu.stratosphere.meteor.server.executor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import eu.stratosphere.meteor.SchedulerConfigConstants;
import eu.stratosphere.meteor.common.MessageBuilder;
import eu.stratosphere.meteor.server.DOPAScheduler;
import eu.stratosphere.meteor.server.ServerConnectionFactory;

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
	 * Max block size 50MB
	 */
	private final int MAX_BLOCK_SIZE = 50 * 1024 * 1024;
	
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
	
	private byte[] fileBytes;
	
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
		
		// save desired block size from client and choose 
		int disiredBlockSize = MessageBuilder.getDesiredBlockSize(request);
		blockSize = disiredBlockSize >= MAX_BLOCK_SIZE ? MAX_BLOCK_SIZE : disiredBlockSize;
		
		sumOfBlocks = MessageBuilder.getMaxNumOfBlocks(request);
		fileIndex = MessageBuilder.getFileIndex(request);
		
//		file = getFile( job );
//		fileBytes = file.getBytes( charset ); // TODO may to big?
//		
//		if ( fileBytes.length/blockSize > maxBlockNumber ){
//			blockSize = (int) (fileBytes.length/maxBlockNumber); // +/- one?
//		} else sumOfBlocks = (fileBytes.length+1) / blockSize; // same here as well
	}
	
	/**
	 * Start parallel thread.
	 */
	@Override
	public void run(){
		// open connection to HDFS
		
		
		// choose definitions and send informations to client
		JSONObject obj = MessageBuilder.buildRequestResult(
				job.getClientID(), 
				job.getJobID(), 
				fileIndex, 
				blockSize,
				sumOfBlocks);
		
		// first send json with informations
		try {
			connFac.replyRequest( requestProps, obj );
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
		
		// send block per block
		for ( int idx = 0; idx < sumOfBlocks; idx++ ){
			try {
				// TODO length? and more efficient way! (may with ByteBuffer)
				connFac.sendBlock(requestProps, Arrays.copyOfRange(fileBytes, idx*blockSize, (idx+1)*blockSize));
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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
