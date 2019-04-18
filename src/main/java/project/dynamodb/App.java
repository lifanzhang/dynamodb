package project.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.core.Response;

import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
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
    	
//    	String data1 = "{\"imageId\":\"image-123\"}";
    	String data= "{\"Name\":\"image-123\", \"Sex\":\"M\", \"Age\":\"23\", \"Height (in)\":\"74\", \"Weight (lbs)\":\"170\"}";
    		
//    	{"Name":"image-123", "Sex":"M", "Age":"23", "Height (in)":74, "Weight (lbs)":170};

    	String tableName = "Biometrics";
    	
    	AWSDynamoDB dynamoDBHelper = new AWSDynamoDB(hostname, signingRegion);

    	List<String> tableAttributes = new ArrayList<String>();
    	tableAttributes.add("id");
    	
    	
    	String primaryKey = "id";
    	
    	HashMap<String, Object> imageInfoHashMap = new Gson().fromJson(data, new TypeToken<HashMap<String, Object>>(){}.getType());

    	try {
    		//Check if table exists already
    		if(!dynamoDBHelper.doesTableExist(tableName)) {
    			dynamoDBHelper.createTable(tableName, tableAttributes);
    		}
    		else {
    			System.out.println("Table exists. Skipping Table creation.");
    		}
    		//Save data in python using batch write
//    		dynamoDBHelper.saveData(tableName, primaryKey, imageInfoHashMap);
    	}
        catch (Exception e) {
            System.err.println("DynamoDB Exception: ");
            System.err.println(e.getMessage());
        }
    	
//    	System.out.println("Query Item: \n");
//    	
//		Item item = new Item();
//		item = null;
//    	String primaryKeyId = "id";
//    	String primaryKeyValue = "52023020";
//    	
//    	try {
//    		item = dynamoDBHelper.getTableItem(tableName, primaryKeyId, primaryKeyValue);
//    	}
//    	catch (Exception e) {
//            System.err.println("DynamoDB Exception: ");
//            System.err.println(e.getMessage());
//    	}
    	
        try {
        	ScanResult result = dynamoDBHelper.scanAndFilterTable(tableName);

        }
        catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
    	
    }
}
