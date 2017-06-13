package mx.iteso.desi.cloud.imageq;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;

public class QueryImages {
  IKeyValueStorage imageStore;
  IKeyValueStorage titleStore;
	
  public QueryImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) 
  {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
	
  public Set<String> query(String word)
  {
      
    HashSet<String> ret = new HashSet<>();
    
    String stem = PorterStemmer.stem(word);
    String term = (stem.equals("Invalid Term")) ? word : stem;
    
    for(String category : titleStore.get(term)) {
        for(String url : imageStore.get(category)) {
            ret.add(url);
        }
    }
      
    return ret;
  }
        
  public void close()
  {
    // TODO: Close the databases
  }
	
  public static void main(String args[]) throws UnknownHostException 
  {
    // TODO: Add your own name here
    System.out.println("*** Alumno: _____________________ (Exp: _________ )");
    
    // TODO: get KeyValueStores
      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
    			"images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
  			"terms");
    
    QueryImages myQuery = new QueryImages(imageStore, titleStore);

    for (int i=0; i<args.length; i++) {
      System.out.println(args[i]+":");
      Set<String> result = myQuery.query(args[i]);
      Iterator<String> iter = result.iterator();
      while (iter.hasNext()) 
        System.out.println("  - "+iter.next());
    }
    
    myQuery.close();
  }
}

