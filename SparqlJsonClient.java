package com.spirit.DMRE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SparqlJsonClient {
	List<String> returnList = new ArrayList<>();
	public SparqlJsonClient() {
		
	}
	public List<String> generateRequest(String sparql, String rdf) throws IOException {
		JSONObject generatedJSON = new JSONObject();
		generatedJSON.put("sparqlquery", sparql);
		generatedJSON.put("rdf", rdf);
		System.out.println(generatedJSON.toString());
		List<String> returnList = sendPostRequest(generatedJSON);
		return returnList;
	}
	public List<String> sendPostRequest(JSONObject request) throws IOException {
		List<String> retList = null;
		URL url = new URL ("http://localhost:8080/DMRE-TRUNK-SNAPSHOT/DMSE/SPARQL/inference");
		HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		connection.setRequestProperty("Accept", "application/sparql-results+json");
		connection.setDoOutput(true);
		try (OutputStream outputstream = connection.getOutputStream()){
			byte[] input = request.toString().getBytes("utf-8");
			outputstream.write(input,0,input.length);
		}
		try(BufferedReader responseStream = new BufferedReader(
				new InputStreamReader(connection.getInputStream(),"utf-8"))){
					StringBuilder response = new StringBuilder();
					String responseLine = null;
					while((responseLine = responseStream.readLine()) != null){
						response.append(responseLine.trim());
					}
					JSONObject res = new JSONObject(response.toString());
					retList = loopThroughJson(res,"value");		
		}
		return retList;
		
	}
	public List<String> loopThroughJson(JSONObject input, String searchKey) throws JSONException {
		
		Iterator<String> keys = input.keys();
		while(keys.hasNext()) {
		    String key = keys.next();
		    if (input.get(key) instanceof JSONObject) {
		          loopThroughJson((JSONObject) input.get(key), searchKey);
		    }
		    if (input.get(key) instanceof JSONArray) {
		    	JSONArray ja = input.getJSONArray(key);
		    	Iterator <Object> it = ja.iterator();
		    	while(it.hasNext()) {
		    		Object tmp = it.next();
		    		if(tmp instanceof JSONObject) {
		    			loopThroughJson((JSONObject)tmp, searchKey);
		    		}
		    	}
		    	
		    }
		    if (key.equals(searchKey)) {
    			//System.out.println("Searching for: " + searchKey + " Result: " + input.get(key));
    			returnList.add(input.get(key).toString());
    		}
		}
		return returnList;
	}
}
