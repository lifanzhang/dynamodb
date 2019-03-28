package project.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.core.Response;

import org.json.JSONObject;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
    	String hostname = "http://localhost:8000";
    	String signingRegion = "us-east-1";
    	   
//    	JSONObject data = new JSONObject();
//    	data.put("imageId", "image-123");
//    	data.put("imageName", "testImage");
//    	data.put("imageLink","https://image.com");
//    	data.put("metadata", "{\"key\":\"values\"}");
    	
    	String data = "{\"imageId\":\"image-123\"}";
    		
//    	{"imageId":"image-123", "imageName":"testImage", "imageLink":"https://image.com", "metadata":{"key":"values"}};

    	String tableName = "ImageMetadata";
    	
    	AWSDynamoDB dynamoDBHelper = new AWSDynamoDB(hostname, signingRegion);

    	List<String> tableAttributes = new ArrayList<String>();
    	tableAttributes.add("imageId");
//    	tableAttributes.add("imageName");
    	
    	String imageName = "test";
    	String imageId = "1";
    	String imageLink = "testing.com";
    	String metadata = "metadata";
    	//DEBUG
    	System.out.println("Image Name: " + imageName);
    	System.out.println("Image Id: " + imageId);
    	System.out.println("Image Link:" + imageLink);
    	System.out.println("Data received: " + metadata.toString());
    	
    	String primaryKey = "imageId";
    	HashMap<String, Object> imageInfoHashMap = new Gson().fromJson(data, new TypeToken<HashMap<String, Object>>(){}.getType());

    	try {
    		//Check if table exists already
    		if(!dynamoDBHelper.doesTableExist(tableName)) {
    			dynamoDBHelper.createTable(tableName, tableAttributes);
    		}
    		else {
    			System.out.println("Table exists. Skipping Table creation.");
    		}
    		dynamoDBHelper.saveData(tableName, primaryKey, imageInfoHashMap);
    	}
        catch (Exception e) {
            System.err.println("DynamoDB Exception: ");
            System.err.println(e.getMessage());
                    }
    	
    }
}
