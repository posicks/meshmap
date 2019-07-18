package jamsesso.meshmap;

import java.io.File;
import java.util.List;

public interface MeshMapCluster 
{
    public List<Node> getAllNodes();

    public <K, V> MeshMap<K, V> join()
    throws MeshMapException;
    
    
    public <T> T register(Node node)
    throws MeshMapException;
    
    
    public <T> T unregister(Node node);
    
    
    public Node getNodeForKey(Object key);
    
    
    public Node getSuccessorNode();
}
