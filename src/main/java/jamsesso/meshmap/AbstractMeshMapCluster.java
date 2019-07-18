package jamsesso.meshmap;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class AbstractMeshMapCluster implements MeshMapCluster, AutoCloseable
{
    protected final Node self;
    
    protected MeshMapServer server;
    
    @SuppressWarnings("rawtypes")
    protected MeshMap map;
    
    
    public AbstractMeshMapCluster(final Node self)
    {
        this.self = self;
        
        if (self == null)
        {
            throw new IllegalArgumentException("Cannot initialize MeshMap Cluster, Socket Address must be provided");
        }
    }
    
    
    @Override
    @SuppressWarnings({"cast", "unchecked"})
    public <K, V> MeshMap<K, V> join()
    throws MeshMapException
    {
        if (this.map != null)
        {
            return (MeshMap<K, V>) this.map;
        }
        
        register(self);
        
        server = new MeshMapServer(this, self);
        MeshMapImpl<K, V> map = new MeshMapImpl<>(this, server, self);
        
        try
        {
            server.start(map);
            map.open();
        } catch (IOException e)
        {
            throw new MeshMapException("Unable to start the mesh map server", e);
        }
        
        server.broadcast(Message.HI);
        this.map = map;
        
        return map;
    }
    
    
    @Override
    public Node getNodeForKey(Object key)
    {
        int hash = key.hashCode() & Integer.MAX_VALUE;
        List<Node> nodes = getAllNodes();
        
        for (Node node : nodes)
        {
            if (hash <= node.getId())
            {
                return node;
            }
        }
        
        return nodes.get(0);
    }
    
    
    @Override
    public Node getSuccessorNode()
    {
        List<Node> nodes = getAllNodes();
        
        if (nodes.size() <= 1)
        {
            return null;
        }
        
        int selfIndex = Collections.binarySearch(nodes, self, Comparator.comparingInt(Node::getId));
        int successorIndex = selfIndex + 1;
        
        // Find the successor node.
        if (successorIndex > nodes.size() - 1)
        {
            return nodes.get(0);
        }
        return nodes.get(successorIndex);
    }
    
    
    @Override
    public void close()
    throws Exception
    {
        if (this.server != null)
        {
            this.server.broadcast(Message.BYE);
            this.server.close();
            this.server = null;
        }
    }
}
