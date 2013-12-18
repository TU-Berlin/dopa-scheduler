package eu.stratosphere.meteor.common;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * A static class to create json objects for requests and answers on scheduler and client side.
 * To iterate through all possible requests it includes a public static enumeration of request
 * types. If you want to create requests by yourself (or you want to create answers) use this
 * class.
 *
 * @author André Greiner-Petter
 *
 */
public class MessageBuilder {
	
	/**
	 * Enumeration of all request types.
	 *
	 * @author André Greiner-Petter
	 */
	public static enum RequestType {
		JOB_EXISTS, // ask whether this job exists on scheduler or not
		JOB_STATUS, // ask for status of a specified job
		JOB_ABORT, // want to abort a specified job
		GET_LINK, // get result links of a finished job
		REQUEST_RESULT, // get result of a finished job
		ERROR; // error message - not really a request type but a message type
		
		/**
		 * Key for request type itself
		 */
		private static final String KEY = "RequestCode";
		
		/**
		 * Keys for non-specified requests
		 */
		private static final String FDX = "FileIndex";
		private static final String BLOCKSIZE = "DesiredBlockSize";
		private static final String MAXBLOCKS = "MaximumNumberOfBlocks";
		
		/**
		 * Keys for specified requests
		 */
		private static final String CID = "ClientID";
		private static final String JID = "JobID";
		private static final String JST = "JobStatus";
		private static final String PAT = "Path";
		private static final String ERR = "Error";
		
		/**
		 * Returns the request type object by a specified JSONObject.
		 * 
		 * @param req request json object. Needs to contain RequestCode
		 * @return request type of specified json object
		 */
		public static RequestType getRequestType( JSONObject req ){
			try{ // use valueOf
				return RequestType.valueOf( req.get(KEY).toString() );
			} catch ( JSONException e ){ return null; }
		}
		
		/**
		 * Returns a json object with given informations.
		 * 
		 * @param clientID can be null
		 * @param jobID can be null
		 * @return json object with given informations and RequestCode
		 */
		private JSONObject createJSONRequest( String clientID, String jobID  ) {
			JSONObject obj = new JSONObject();
			
			// put informations
			try {
				obj.put( KEY, this.toString() );
				obj.put( CID, clientID );
				obj.put( JID, jobID );
			} catch ( JSONException e ){}
			
			return obj;
		}
	}
	
	/**
	 * Just a static class to build requests and answers represented by json objects.
	 */
	private MessageBuilder(){}
	
	/**
	 * Returns the json object to ask whether this job exists or not.
	 * @param clientID of client
	 * @param jobID of job
	 * @return json request object
	 */
	public static JSONObject buildJobExistsRequest( String clientID, String jobID ){
		return RequestType.JOB_EXISTS.createJSONRequest(clientID, jobID);
	}
	
	/**
	 * Returns the json object to ask status of the specified job.
	 * @param clientID of client
	 * @param jobID of job
	 * @return json request object
	 */
	public static JSONObject buildJobStatus( String clientID, String jobID, JobState status ){
		try {
			JSONObject obj = RequestType.JOB_STATUS.createJSONRequest(clientID, jobID);
			obj.put( RequestType.JST, status );
			return obj;
		} catch (JSONException e) { return null; }
	}
	
	/**
	 * Build an error status object with given error message
	 * @param clientID
	 * @param jobID
	 * @param errorMessage
	 * @return json request object
	 */
	public static JSONObject buildErrorStatus( String clientID, String jobID, String errorMessage ){
		try {
			JSONObject obj = MessageBuilder.buildJobStatus(clientID, jobID, JobState.ERROR);
			obj.put( RequestType.ERR, errorMessage );
			return obj;
		} catch (JSONException e) { return null; }
	}
	
	/**
	 * Returns the json object to try to abort a specified job.
	 * @param clientID of client
	 * @param jobID of job
	 * @return json request object
	 */
	public static JSONObject buildJobAbort( String clientID, String jobID ){
		return RequestType.JOB_ABORT.createJSONRequest(clientID, jobID);
	}
	
	/**
	 * Returns the json object to ask for a result link.
	 * @param clientID of this client
	 * @param jobID of this job
	 * @param fileIndex of specified output file of job
	 * @return json request object
	 */
	public static JSONObject buildGetLink( String clientID, String jobID, int fileIndex ){
		try { 
			JSONObject obj = RequestType.GET_LINK.createJSONRequest(clientID, jobID);
			obj.put( RequestType.FDX, fileIndex );
			return obj;
		} catch (JSONException e) { return null; }
	}
	
	/**
	 * Returns the json object to get result of finished job.
	 * @param clientID of client
	 * @param jobID of job
	 * @param fileIndex of specified output file of job
	 * @param desiredBlockSize of files
	 * @param maxNumOfBlocks of files
	 * @return json request object
	 */
	public static JSONObject buildRequestResult( String clientID, String jobID, int fileIndex, int desiredBlockSize, long maxNumOfBlocks ){
		JSONObject obj = RequestType.REQUEST_RESULT.createJSONRequest(clientID, jobID);
		
		try {
			obj.put( RequestType.FDX, fileIndex );
			obj.put( RequestType.BLOCKSIZE, desiredBlockSize );
			obj.put( RequestType.MAXBLOCKS, maxNumOfBlocks );
		} catch ( JSONException e ){}
		
		return obj;
	}
	
	/**
	 * Creates an error message as a json object to a specified client.
	 * @param clientID receiver
	 * @param errorMessage message
	 * @return JSONObject with error message
	 */
	public static JSONObject buildJoblessError( String clientID, String errorMessage ){
		JSONObject obj = RequestType.ERROR.createJSONRequest(clientID, "");
			try {
			// remove the not used jobID
			obj.remove(RequestType.JID);
			obj.put( RequestType.ERR, errorMessage);
		} catch ( JSONException e ){}

		return obj;
	}

	/**
	 * Adds the path information to specified json object.
	 * @param obj get new input
	 * @param path input itself
	 * @return given json object with added path information
	 */
	public static JSONObject addPath( JSONObject obj, String path ){
		try { return obj.put( RequestType.PAT, path ); }
		catch (JSONException e) { return obj;	}
	}
	
	/**
	 * Returns client ID
	 * @param request
	 * @return client ID
	 */
	public static String getClientID( JSONObject request ){
		try { return request.getString( RequestType.CID ); } 
		catch (JSONException e) { return null; }
	}
	
	/**
	 * Returns job ID
	 * @param request
	 * @return job ID
	 */
	public static String getJobID( JSONObject request ){
		try { return request.getString( RequestType.JID ); } 
		catch (JSONException e) { return null; }
	}
	
	/**
	 * Returns job status
	 * @param request
	 * @return JobState
	 */
	public static JobState getJobStatus( JSONObject request ){
		try { 
			String jobStateString = request.getString( RequestType.JST );
			if ( jobStateString == null ) return null;
			return JobState.valueOf(jobStateString);
		} catch (JSONException e) { return null; }
	}
	
	/**
	 * Returns path
	 * @param request
	 * @return path
	 */
	public static String getLink( JSONObject request ){
		try { return request.getString( RequestType.PAT ); }
		catch (JSONException e) { return null; }
	}
	
	/**
	 * Returns the error message
	 * @param request
	 * @return error message
	 */
	public static String getErrorMessage( JSONObject request ){
		try { return request.getString( RequestType.ERR ); }
		catch (JSONException e) { return null; }
	}
	
	/**
	 * Returns fileIndex
	 * @param request
	 * @return fileIndex or -1 if an error occurred
	 */
	public static int getFileIndex( JSONObject request ){
		try { return request.getInt( RequestType.FDX ); } 
		catch (JSONException e) { return -1; }
	}
	
	/**
	 * Returns desired block size for answers
	 * @param request
	 * @return desired block size in bytes or -1 if an error occurred
	 */
	public static int getDesiredBlockSize( JSONObject request ){
		try { return request.getInt( RequestType.BLOCKSIZE ); } 
		catch (JSONException e) { return -1; }
	}
	
	/**
	 * Returns maximum number of blocks
	 * @param request
	 * @return maximum number of blocks or -1 if an error occurred
	 */
	public static long getMaxNumOfBlocks( JSONObject request ){
		try { return request.getLong( RequestType.MAXBLOCKS ); } 
		catch (JSONException e) { return -1; }
	}
}
