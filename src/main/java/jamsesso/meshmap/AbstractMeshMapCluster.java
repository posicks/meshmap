package jamsesso.meshmap;
 
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
 
public abstract class AbstractMeshMapCluster implements MeshMapCluster, AutoCloseable
{
    private static final Message HI = new Message(Message.TYPE_HI);
   
    private static final Message BYE = new Message(Message.TYPE_BYE);
   
    private static final Message ACK = new Message(Message.TYPE_ACK);
   
    private static final Message ERR = new Message(Message.TYPE_ERR);
   
    private static final Message YES = new Message(Message.TYPE_YES);
   
    private static final Message NO = new Message(Message.TYPE_NO);
 
    private static final Message NOOP = new Message(Message.TYPE_NOOP);
   
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
   
    
    public Node getSelf()
    {
        return self;
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
        this.map = map;
       
        try
        {
            server.start(map);
            map.open();
        } catch (IOException e)
        {
            throw new MeshMapException("Unable to start the mesh map server", e);
        }
       
        server.broadcast(messageHI());
       
        return map;
    }
   
    
    public Message messageHI()
    {
        return HI;
    }
   
    
    public Message messageBYE()
    {
        return BYE;
    }
   
    
    public Message messageACK()
    {
        return ACK;
    }
   
    
    public Message messageERR()
    {
        return ERR;
    }
   
    
    public Message messageYES()
    {
        return YES;
    }
   
    
    public Message messageNO()
    {
        return NO;
    }
   
    
    public Message messageNOOP()
    {
        return NOOP;
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
            this.server.broadcast(messageBYE());
            this.server.close();
            this.server = null;
        }
    }
}