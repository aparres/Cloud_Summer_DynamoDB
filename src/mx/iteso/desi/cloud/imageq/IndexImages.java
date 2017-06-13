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
            ParseTriples imagesParser = new ParseTriples(imageFileName);

            //Code to add images to imageStore
            
            
            //Start Parcing Labels File
            ParseTriples termsParser = new ParseTriples(titleFileName);
            //Code to add images to titleStore

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

