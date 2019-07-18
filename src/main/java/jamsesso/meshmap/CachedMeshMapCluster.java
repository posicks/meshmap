package jamsesso.meshmap;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class CachedMeshMapCluster implements MeshMapCluster
{
    private final ReentrantLock lock = new ReentrantLock();
    
    private final MeshMapCluster delegate;
    
    private List<Node> nodes;

    private Node successorNode;

    private HashMap<Object, Node> nodeMap;
    
    
    public CachedMeshMapCluster(MeshMapCluster cluster)
    {
        this.delegate = cluster;
    }
    
    
    @Override
    public List<Node> getAllNodes()
    {
        lock.lock();
        try
        {
            if (nodes == null)
            {
                nodes = delegate.getAllNodes();
            }
            
            return nodes;
        } finally
        {
            lock.unlock();
        }
    }
    
    
    @Override
    public <K, V> MeshMap<K, V> join()
    throws MeshMapException
    {
        lock.lock();
        try
        {
            clearCache();
            return delegate.join();
        } finally
        {
            lock.unlock();
        }
    }
    
    
    public void clearCache()
    {
        lock.lock();
        try
        {
            nodes = null;
            successorNode = null;
            nodeMap = null;
        } finally
        {
            lock.unlock();
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public File register(Node node)
    throws MeshMapException
    {
        lock.lock();
        try
        {
            clearCache();
            return delegate.register(node);
        } finally
        {
            lock.unlock();
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public File unregister(Node node)
    {
        lock.lock();
        try
        {
            clearCache();
            return delegate.unregister(node);
        } finally
        {
            lock.unlock();
        }
    }


    @Override
    public Node getNodeForKey(Object key)
    {
        lock.lock();
        try
        {
            Node node = null;

            if (this.nodeMap == null)
            {
                this.nodeMap = new HashMap<Object, Node>();
            }
            
            if ((node = this.nodeMap.get(key)) == null)
            {
                this.nodeMap.put(key, node = delegate.getNodeForKey(key));
            }
            
            return node;
        } finally
        {
            lock.unlock();
        }
    }


    @Override
    public Node getSuccessorNode()
    {
        lock.lock();
        try
        {
            if (this.successorNode == null)
            {
                this.successorNode = delegate.getSuccessorNode();
            }
            
            return this.successorNode;
        } finally
        {
            lock.unlock();
        }
    }
}
