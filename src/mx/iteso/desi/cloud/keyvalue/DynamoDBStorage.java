package mx.iteso.desi.cloud.keyvalue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import mx.iteso.desi.cloud.imageq.Config;


public class DynamoDBStorage extends BasicKeyValueStore {

    String dbName;
    AmazonDynamoDB ddb;
    int inx;

    Set<String> attributesToGet = new HashSet<String>();

    public DynamoDBStorage(String dbName) {
        this.dbName = dbName;
        this.attributesToGet.add("value");
        
        //Conecction
        BasicAWSCredentials cred = new BasicAWSCredentials(Config.accessKeyID,Config.secretAccessKey);
        this.ddb = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(cred)).withRegion(Config.amazonRegion).build();
        init();
    }

    private void init()  {
        //Validate if table exists
        if(!this.ddb.listTables().getTableNames().contains(dbName)) {
            //La tabla no existe hay que crearla.
            System.out.println("Table does not exists, creating it...");
            
            CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(
                    new AttributeDefinition().withAttributeName("keyword").withAttributeType(ScalarAttributeType.S),
                    new AttributeDefinition().withAttributeName("inx").withAttributeType(ScalarAttributeType.N))
                .withKeySchema(
                    new KeySchemaElement().withAttributeName("keyword").withKeyType(KeyType.HASH),
                    new KeySchemaElement().withAttributeName("inx").withKeyType(KeyType.RANGE))
                .withProvisionedThroughput(new ProvisionedThroughput(new Long(5), new Long(5)))
                .withTableName(dbName);            
            
            this.ddb.createTable(request);
            
            try {
                //Esperamos a que la tabla se cree
                TableUtils.waitUntilActive(ddb, dbName);
            } catch (InterruptedException ex) {
                Logger.getLogger(DynamoDBStorage.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            } catch (TableUtils.TableNeverTransitionedToStateException ex) {
                Logger.getLogger(DynamoDBStorage.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            }
            
            System.out.println("Table created and ready...");
        }
        
        ScanRequest scanRequest = new ScanRequest().withTableName(this.dbName);
        scanRequest.setSelect(Select.COUNT);
        
        ScanResult res = this.ddb.scan(scanRequest);
        this.inx = res.getCount()+1;
        
        System.out.println("Connection with table "+dbName+" ready (INX set to: "+this.inx+")");
        
    }
    
    @Override
    public Set<String> get(String search) {
        HashSet<String> ret = new HashSet<>();

        HashMap<String, Condition> filter = new HashMap<String, Condition>();
        
        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(search));
        
        filter.put("keyword", condition);
        
        QueryRequest queryRequest = new QueryRequest().
                withTableName(this.dbName).withKeyConditions(filter);
        
        List <Map<String,AttributeValue>> result = this.ddb.query(queryRequest).getItems();
        
        for(Map<String,AttributeValue> r : result) {
            System.out.println("Value: "+r.get("value").getS());
            ret.add(r.get("value").getS());
        }
        
        return ret;
    }

    @Override
    public boolean exists(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<String> getPrefix(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addToSet(String keyword, String value) {
        put(keyword, value);
    }

    @Override
    public void put(String keyword, String value) {
        Map<String, AttributeValue> entry = new HashMap<String, AttributeValue>();

        entry.put("keyword", new AttributeValue(keyword));
        entry.put("inx", new AttributeValue().withN(Integer.toString(this.inx++)));
        entry.put("value", new AttributeValue(value));

        PutItemRequest putItemRequest = new PutItemRequest(dbName, entry);
        this.ddb.putItem(putItemRequest);
    }
    
    @Override
    public void put(Map<String, String> entries) {
        
        int totalCounter = entries.size();
        System.out.println("Adding "+totalCounter+" with Batch Write");

        
        ArrayList<WriteRequest> requestList = new ArrayList();
        
        int counter = 0;
        int maxCounter = 25;
        int writedElements = 0;
        for(Entry<String, String> e : entries.entrySet()) {
        
            Map<String, AttributeValue> entry = new HashMap<>();
            entry.put("keyword", new AttributeValue(e.getKey()));
            entry.put("inx", new AttributeValue().withN(Integer.toString(this.inx++)));
            entry.put("value", new AttributeValue(e.getValue()));
            WriteRequest request = new WriteRequest(new PutRequest().withItem(entry));
            requestList.add(request);
            counter++;
            
            if(counter == maxCounter) {
                System.out.println("Writing "+counter+" elements");
                writeBatchItems(requestList);
                writedElements += counter;
                System.out.println((totalCounter-writedElements)+" elementos faltantes");
                counter = 0;
                requestList.clear();
            }
        }
        
        if (!requestList.isEmpty()) {
            System.out.println("Writing " + counter + " elements");
            writeBatchItems(requestList);
            writedElements += counter;
            System.out.println((totalCounter - writedElements) + " elementos faltantes");
        }
            
    }
    
    private void writeBatchItems(ArrayList<WriteRequest> requestList) {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        
        requestItems.put(dbName, requestList);
        BatchWriteItemRequest bwir = new BatchWriteItemRequest(requestItems);
        BatchWriteItemResult result  = this.ddb.batchWriteItem(bwir);

        do {
            Map<String, List<WriteRequest>> unprocessItems = result.getUnprocessedItems();
            if(unprocessItems.size() > 0) {
                System.out.println("\t"+unprocessItems.size()+" of request not processed");
                result = this.ddb.batchWriteItem(unprocessItems);
            } else {
                System.out.println("\tAll request processed");
            }
        } while (result.getUnprocessedItems().size() > 0);
    }
    

    @Override
    public void close() {
    }
    
    @Override
    public boolean supportsPrefixes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sync() {
    }

    @Override
    public boolean isCompressible() {
        return false;
    }

    @Override
    public boolean supportsMoreThan256Attributes() {
        return true;
    }

}
