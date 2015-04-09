package dynamo;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.Maps;

public class Run {
	
	private static final String INPUT_FILE_PATH = "input.txt";
	private static final String TABLE_NAME = null;

	@Test
	public void pollDynamo() throws Exception {
		ProfileCredentialsProvider profileCredentialsProvider = new ProfileCredentialsProvider();		
		AmazonDynamoDBClient client = new AmazonDynamoDBClient(profileCredentialsProvider);
		client.setRegion(Region.getRegion(Regions.EU_WEST_1));
		DynamoDB dynamoDB = new DynamoDB(client);
		
		Table table = dynamoDB.getTable(TABLE_NAME);		
		Map<String, String> keys = readCSVFile(INPUT_FILE_PATH);
		
		int count = 1;
		for (String source : keys.keySet()) {
			String destination = keys.get(source).trim();
			System.out.println(count + ": " + source + " -> " + destination);
			Item item = new Item();
			item.withString("source", source.trim());
			item.withString("destination", destination);
			table.putItem(item);						
			Thread.sleep(100);
			count++;
		}
	}
	
	public Map<String, String> readCSVFile(String path) throws IOException {
		Map<String, String> keys = Maps.newHashMap();
		Reader in = new FileReader(path);
		Iterable<CSVRecord> records = CSVFormat.TDF.parse(in);
		for (CSVRecord record : records) {
			String key = record.get(0);
			String value = record.get(1);
			keys.put(key, value);			
		}
		return keys;
	}
	
}