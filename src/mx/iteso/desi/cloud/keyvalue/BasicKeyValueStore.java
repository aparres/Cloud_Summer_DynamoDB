package mx.iteso.desi.cloud.keyvalue;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public abstract class BasicKeyValueStore implements IKeyValueStorage {

	@Override
	public void addToSet(String keyword, Set<String> values) {
		for (String str: values)
			addToSet(keyword, str);
	}

    @Override
    public void put(Map<String, String> entries) {
        for (Entry<String, String> e : entries.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }
}        
