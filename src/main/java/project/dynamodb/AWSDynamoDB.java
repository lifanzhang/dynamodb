package project.dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONString;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

public class AWSDynamoDB {

	private static AmazonDynamoDB client;
	private static DynamoDB dynamoDB;
	private static String serviceEndpoint;
	private static String signingRegion;
	
	public AWSDynamoDB(String serviceEndpoint, String signingRegion) {
		AWSDynamoDB.serviceEndpoint = serviceEndpoint;
		AWSDynamoDB.signingRegion = signingRegion;
		
		client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, signingRegion))
                .build();
        
        dynamoDB = new DynamoDB(client);
	}
	
	public JSONObject createTable(String tableName, List<String> tableAttributes) {
		
//		client = AmazonDynamoDBClientBuilder.standard()
//                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, signingRegion))
//                .build();
//        
//        dynamoDB = new DynamoDB(client);
        
		JSONObject response = new JSONObject();
		
        List<KeySchemaElement> KeySchemaElementList = new ArrayList<KeySchemaElement>();
        List<AttributeDefinition> AttributeDefinitionList = new ArrayList<AttributeDefinition>();
        
        
        for(int i = 0; i < tableAttributes.size(); i++) {
        	//If first element in table list, it is the partition key.
        	String key = tableAttributes.get(i);
        	if(i == 0) {
        		KeySchemaElementList.add(new KeySchemaElement(key, KeyType.HASH));
        		//Assigns the data type (String, Numeric, Binary) for each Attribute in Attribute List
        		AttributeDefinitionList.add(new AttributeDefinition(key, ScalarAttributeType.S));
        		continue;
        	}
        	
//        	//Else, it is a sort key
//    		KeySchemaElementList.add(new KeySchemaElement(key, KeyType.RANGE));
//    		AttributeDefinitionList.add(new AttributeDefinition(key, ScalarAttributeType.S));
        }
        
        //DEBUG
//        System.out.println(KeySchemaElementList.toString());
        
        try {
            System.out.println("Attempting to create table " + tableName + "; please wait...");
            
            //Hardcoded way to create a dynamoDB table
//            Table table = dynamoDB.createTable(tableName,
//                    Arrays.asList(new KeySchemaElement("imageId", KeyType.HASH), // Partition key
//                        new KeySchemaElement("imageName", KeyType.RANGE)), // Sort key
//                    Arrays.asList(new AttributeDefinition("imageId", ScalarAttributeType.S),
//                        new AttributeDefinition("imageName", ScalarAttributeType.S)),
//                    new ProvisionedThroughput(10L, 10L));
          
//            //Generic way to create a dynamoDB table
        	Table table = dynamoDB.createTable(tableName, 
				KeySchemaElementList, 
				AttributeDefinitionList, 
				new ProvisionedThroughput(10L, 10L));
            
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
            
            response.put("status code", Defines.HTTP400);
            response.put("error message", e.getMessage());
            return response;
        }
        
		response.put("status code", Defines.HTTP200);
		return response;
	}
	
//	public void saveMetadata(String tableName, String imageId, String imageName, String imageLink, String metadata) {
//		
//		Table table = dynamoDB.getTable("ImageMetadata");
//		
//		try {
//			System.out.println("Adding data to " + tableName);
//			
//			Item item = new Item().withPrimaryKey("imageId", imageId).withString("imageName", imageName).withString("imageLink", imageLink).withString("metadata", metadata);
//			table.putItem(item);
//		}
//        catch (Exception e) {
//            System.err.println("Failed to create item in " + tableName);
//            System.err.println(e.getMessage());
//        }
//	}
//
//	public void saveDeploymentInfo(String tableName, String deploymentId, String serviceName, String deploymentData) {
//		Table table = dynamoDB.getTable("DeploymentData");
//		
//		try {
//			System.out.println("Adding data to " + tableName);
//			
////			Item item = new Item().withPrimaryKey("deploymentId", deploymentId).withString("serviceName", serviceName).withString("deploymentData", deploymentData);
//			Item item = new Item().withPrimaryKey("deploymentId", deploymentId).withString("serviceName", serviceName);
//			item = item.withString("deploymentData", deploymentData);
//
//			table.putItem(item);
//		}
//        catch (Exception e) {
//            System.err.println("Failed to create item in " + tableName);
//            System.err.println(e.getMessage());
//        }
//	}
	
	//CreateTableColumnsAndSavesData
	//Might need refactoring
	public JSONObject saveData(String tableName, String primaryKey, HashMap<String, Object> deploymentHashMap) {
		Table table = dynamoDB.getTable(tableName);
		
		JSONObject response = new JSONObject();
		
		try {	
			System.out.println("Adding data to " + tableName);
			
//			Item item = new Item();
			Item item;
			
			System.out.println(primaryKey);
			System.out.println(deploymentHashMap.get(primaryKey));
			
			
			//Check if primary key exists in table
			if(table.getItem(primaryKey, deploymentHashMap.get(primaryKey)) != null){
				//If primary key exists, return an error saying that it exists
				response.put("status code", Defines.HTTP400);
				response.put("error message", Defines.ELEMENTEXISTS);
				return response;
			}
			else {
				//If primary key doesn't exist, create a new item to be inserted into table
				item = new Item();
			}
			
			System.out.println(item.toJSONPretty());
			
			for(String key: deploymentHashMap.keySet()) {
				if(key.equals(primaryKey)) {
					item.withPrimaryKey(key, deploymentHashMap.get(key));
					continue;
				}
				//Insert rest of columns and data into dynamoDB
				//Can't cast to String
				item = item.withString(key, deploymentHashMap.get(key).toString());
			}
			table.putItem(item);
		}
        catch (Exception e) {
            System.err.println("Failed to create item in " + tableName);
            System.err.println(e.getMessage());
            
            response.put("status code", Defines.HTTP400);
            response.put("error message", e.getMessage());
            return response;
        }
		
		response.put("status code", Defines.HTTP200);
		return response;
	}
	
	//Updates an existing primaryKey with new values
	public JSONObject updateData(String tableName, String primaryKey, HashMap<String, Object> deploymentHashMap) {
		Table table = dynamoDB.getTable(tableName);
		
		JSONObject response = new JSONObject();
		
		try {	
			System.out.println("Adding data to " + tableName);
			
//			Item item = new Item();
			Item item;
			
			System.out.println(primaryKey);
			System.out.println(deploymentHashMap.get(primaryKey));
			
			
			//Check if primary key exists in table
			if(table.getItem(primaryKey, deploymentHashMap.get(primaryKey)) != null){
				//If primary key exists, set item to existing item in table matching the primary key
				item = table.getItem(primaryKey, deploymentHashMap.get(primaryKey));
			}
			else {
				//If primary key doesn't exist, create a new item to be inserted into table
				item = new Item();
			}
			
			System.out.println(item.toJSONPretty());
			
			for(String key: deploymentHashMap.keySet()) {
				if(key.equals(primaryKey)) {
					item.withPrimaryKey(key, deploymentHashMap.get(key));
					continue;
				}
				//Insert rest of columns and data into dynamoDB
				//Can't cast to String
				item = item.withString(key, deploymentHashMap.get(key).toString());
			}
			table.putItem(item);
		}
        catch (Exception e) {
            System.err.println("Failed to create item in " + tableName);
            System.err.println(e.getMessage());
            
            response.put("status code", Defines.HTTP400);
            response.put("error message", e.getMessage());
            return response;
        }
		
		response.put("status code", Defines.HTTP200);
		return response;
	}
	
	public boolean doesTableExist(String tableName) {
		try {
			TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
			System.out.println("Table description: " + tableDescription.getTableStatus());
			return true;
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.err.println("Table does not exist: " + tableName);
			return false;
		}
	}
	
	public Item getTableItem(String tableName, String primaryKeyId, String primaryKeyValue) {
		Table table = dynamoDB.getTable(tableName);
		
		Item item = new Item();
		item = null;
		
		//Another Implementation
//		GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKeyId, primaryKeyValue);
//		Item item = table.getItem(spec);

		/*
		//If your primaryKey has both a Partition Key AND a Sort Key
		KeyAttribute keyAttribute1 = new KeyAttribute("deploymentId", "deploy-123");
		KeyAttribute keyAttribute2 = new KeyAttribute("serviceName", "testApp");
		PrimaryKey primaryKey = new PrimaryKey(keyAttribute1, keyAttribute2);
		Item item = table.getItem(primaryKey);
		*/
		
		try {
//			item = table.getItem("deploymentId", "deploy-123");
			item = table.getItem("deploymentId", primaryKeyValue);
			System.out.println("Displaying retrieved items...");
			System.out.println(item.toJSONPretty());
			
		} catch (Exception e) {
			System.err.println("Cannot retrieve items.");
			System.err.println(e.getMessage());
		}
		
		return item;
	}
	
	public JSONObject updateTableItem(String tableName, String primaryKeyId, String attributeName, String updateValue) {
		Table table = dynamoDB.getTable(tableName);
		JSONObject response = new JSONObject();
		
		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("deploymentId", primaryKeyId)
				.withUpdateExpression("set " + attributeName +" = :newValue")
				.withValueMap(new ValueMap()
						.withString(":newValue", updateValue));
		try {
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			System.out.println("UpdateItem succeeded:\n" + outcome.toString());

		}
		catch (Exception e) {
			System.err.println("Unable to update item: " + attributeName);
            System.err.println(e.getMessage());
            
            response.put("status code", Defines.HTTP400);
            response.put("error message", e.getMessage());
            return response;
		}
		
		response.put("status code", Defines.HTTP200);
		return response;
		
	}
	
	public JSONObject deleteTable(String tableName) {
		JSONObject response = new JSONObject();
		
		if (this.doesTableExist(tableName)) {
			Table table = dynamoDB.getTable(tableName);
			try {
				table.delete();

	            System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");

	            table.waitForDelete();
	            
	            response.put("status code", Defines.HTTP200);
	          
				
			} catch (Exception e){
				System.err.println("DeleteTable request failed for " + tableName);
	            System.err.println(e.getMessage());
	            response.put("status code", Defines.HTTP400);
	            response.put("error message", e.getMessage());
			}
		}
		else {
			response.put("status code", Defines.HTTP400);
			response.put("error message", "Table does not exist");
		}
		return response;
	}
	
//	public List<Object> scanTable(String tableName) {
//	public ItemCollection<ScanOutcome> scanTable(String tableName) {
	public JSONArray scanTable(String tableName) {
    	    	
    	Table table = dynamoDB.getTable(tableName);
    	ScanSpec scanSpec = new ScanSpec();
    	List<Object> tableList = new ArrayList<Object>();  
    	JSONArray array = new JSONArray();
    	JSONObject object = new JSONObject();
    	ItemCollection<ScanOutcome> items = null;
    	try {
    		items = table.scan(scanSpec);
    		
    		Iterator<Item> iter = items.iterator();
    		
			//First scan from table
			System.out.println("First scan from table: ");
			
    		while (iter.hasNext()) {
    			Item item = iter.next();
    			System.out.println(item.toJSON());
    			tableList.add(item.toJSON());
//    			object = new JSONObject(item.toString());
    			array.put(item.toJSON());
    		}
    	}
    	catch(Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
    	}
    	
//		return tableList;
//    	return items;
    	return array;
	}
}


















