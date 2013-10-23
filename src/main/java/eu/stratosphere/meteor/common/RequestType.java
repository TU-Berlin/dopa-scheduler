package eu.stratosphere.meteor.common;

import org.json.JSONException;
import org.json.JSONObject;

public enum RequestType {
	JOB_EXISTS,
	JOB_STATUS;
	
	private final String KEY = "RequestCode";
	private final String CID = "ClientID";
	private final String JID = "JobID";
	
	/**
	 * Returns the request type object by a specified JSONObject.
	 * 
	 * @param req request json object. Needs to contain RequestCode
	 * @return request type of specified json object
	 */
	public RequestType getRequestType( JSONObject req ){
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
	public JSONObject createJSONRequest( String clientID, String jobID ) {
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
