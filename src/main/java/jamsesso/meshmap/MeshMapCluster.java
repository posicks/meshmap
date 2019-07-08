package jamsesso.meshmap;

import java.util.List;

public interface MeshMapCluster 
{
    public List<Node> getAllNodes();

    public <K, V> MeshMap<K, V> join()
    throws MeshMapException;
}
