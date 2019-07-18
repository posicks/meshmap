package jamsesso.meshmap;

import java.util.Map;

public interface MeshMap<K, V> extends Map<K, V>, AutoCloseable
{
    static final String TYPE_PUT = "PUT";
    
    static final String TYPE_GET = "GET";
    
    static final String TYPE_REMOVE = "REMOVE";
    
    static final String TYPE_CLEAR = "CLEAR";
    
    static final String TYPE_KEY_SET = "KEY_SET";
    
    static final String TYPE_SIZE = "SIZE";
    
    static final String TYPE_CONTAINS_KEY = "CONTAINS_KEY";
    
    static final String TYPE_CONTAINS_VALUE = "CONTAINS_VALUE";
    
    static final String TYPE_DUMP_ENTRIES = "DUMP_ENTRIES";
}
