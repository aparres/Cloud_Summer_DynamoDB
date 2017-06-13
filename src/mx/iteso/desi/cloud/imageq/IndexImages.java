package mx.iteso.desi.cloud.imageq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.ParseTriples;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;
import mx.iteso.desi.cloud.keyvalue.Triple;

public class IndexImages {
  ParseTriples parser;
  IKeyValueStorage imageStore, titleStore;
    
  public IndexImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
      
  public void run(String imageFileName, String titleFileName) throws IOException
  {
            Triple triple;
     
            //Start Parcing Images File
            int counter = 0;
            ParseTriples imagesParser = new ParseTriples(imageFileName);
            Map<String, String> imagesEntries = new HashMap<>();
            while((triple = imagesParser.getNextTriple())!=null) {
                if(!triple.getPredicate().equals("http://xmlns.com/foaf/0.1/depiction")) {
                    continue;
                }
                if(!Config.filter.isEmpty()) {
                    if(!triple.getSubject().substring(triple.getSubject().lastIndexOf("/")+1).startsWith(Config.filter)) {
                        continue;
                    }
                }
                
                imagesEntries.put(triple.getSubject(), triple.getObject());
                counter++;
            }
            
            imageStore.put(imagesEntries);
            System.out.println(counter+" images added to store");
            
            
            //Start Parcing Labels File
            counter=0;
            ParseTriples termsParser = new ParseTriples(titleFileName);
            Map<String, String> termsEntries = new HashMap<>();
            while((triple = termsParser.getNextTriple())!=null) {
                if(!triple.getPredicate().equals("http://www.w3.org/2000/01/rdf-schema#label")) {
                    continue;
                }
                if(!imagesEntries.containsKey(triple.getSubject())) {
                    continue;
                }
                
                for(String word : triple.getObject().split(" ")) {
                    String steam = PorterStemmer.stem(word);
                    String term = steam.equals("Invalid term") ? word : steam;
                    termsEntries.put(term.toLowerCase(), triple.getSubject());
                    counter++;
                }
            }
            titleStore.put(termsEntries);
            System.out.println(counter+" terms added to store");            
            this.close();
  }
  
  public void close() {
      this.imageStore.close();
      this.titleStore.close();
  }
  
  public static void main(String args[])
  {
    // TODO: Add your own name here
    System.out.println("*** Starting Image Loading");
    try {

      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
    			"images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
  			"terms");


      IndexImages indexer = new IndexImages(imageStore, titleStore);
      indexer.run(Config.imageFileName, Config.titleFileName);
      System.out.println("Indexing completed");
      
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to complete the indexing pass -- exiting");
    }
  }
}

