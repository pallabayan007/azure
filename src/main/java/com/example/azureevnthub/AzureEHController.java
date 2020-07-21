package com.example.azureevnthub;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubException;

@RestController
@RequestMapping(path="/AEH")
public class AzureEHController {
	
	
	@PostMapping(path= "/eventpush", consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> eventPush(@RequestHeader("namespace") String namespace,
												@RequestHeader("ehname") String ehname,
												@RequestHeader("saskeyname") String saskeyname,
												@RequestHeader("saskey") String saskey,
												@RequestBody JsonObject payload){
		
		if(namespace.isEmpty()
				|| ehname.isEmpty()
				|| saskeyname.isEmpty()
				|| saskey.isEmpty()
				|| payload.isJsonNull()) {
			return new ResponseEntity<String>(new Gson().toJson("Either any of the header or request body is missing"), HttpStatus.BAD_REQUEST);
		}
		
		final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName(namespace) 
                .setEventHubName(ehname)
                .setSasKeyName(saskeyname)
                .setSasKey(saskey);
		try {
			if(AzureEHServices.eventPush(connStr, payload)) {
				return new ResponseEntity<String>(new Gson().toJson("Successfully published into :"+ehname), HttpStatus.OK);
			}
			else{
				throw new ResponseStatusException(
				           HttpStatus.NOT_ACCEPTABLE, "Not able to published into :"+ehname);				
			}
		}catch(Exception e) {
			return new ResponseEntity<String>(new Gson().toJson("Not able to published into :"+ehname), HttpStatus.NOT_ACCEPTABLE);
		}
		
		
	}//End of Event Push
	
	
}
