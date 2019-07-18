/**
 * 
 */
package jamsesso.meshmap;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Steve Posick
 */
public class DiscoveryMeshMapImpl<K, V> extends MeshMapImpl<K, V> implements Handler<Message>
{
    private static Logger LOG = Logger.getLogger(DiscoveryMeshMapImpl.class.getName());

    public DiscoveryMeshMapImpl(MeshMapCluster cluster, MeshMapServer server, Node self)
    {
        super(cluster, server, self);
    }
    
    
    @Override
    public Message handle(Message response)
    {
        try
        {
            switch (response.getType())
            {
                case Message.TYPE_HI:
                case Message.TYPE_BYE:
                {
                    return new Message(Message.TYPE_ACK, cluster.getAllNodes().toArray());
                }
                case Message.TYPE_ACK:
                {
                    Node[] nodes = response.getPayload(Node[].class);
                    if (nodes != null)
                    {
                        for (Node node : nodes)
                        {
                            try
                            {
                                super.cluster.register(node);
                            } catch (MeshMapException e)
                            {
                                LOG.log(Level.SEVERE, "Could not register Node: " + e.getMessage(), e);
                            }
                        }
                    }
                    break;
                }
            }
        } catch (Exception e)
        {
            LOG.log(Level.WARNING, "Error processing Response (" + response + ")", e);
        }
        return super.handle(response);
    }
}