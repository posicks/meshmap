/**
*
 */
package jamsesso.meshmap;
 
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
 
/**
* Discover MeshMap exchanges known nodes during protocol handshake so that all nodes are aware of all other nodes with minimal configuration, such as node chaining.
*
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
                {
                    Node node = response.getNode();
                    List<Node> nodes = cluster.getAllNodes();
                    if (!nodes.contains(node))
                    {
                        cluster.register(node);
                    }
                    processPayload(response);
                    return cluster.messageACK();
                }
                case Message.TYPE_BYE:
                {
                    cluster.unregister(response.getNode());
                    return cluster.messageACK();
                }
                case Message.TYPE_ACK:
                {
                    processPayload(response);
                    break;
                }
            }
        } catch (Exception e)
        {
            LOG.log(Level.WARNING, "Error processing Response (" + response + ")", e);
        }
        return super.handle(response);
    }
 
 
    private void processPayload(Message response)
    {
        Node[] nodes = response.getPayload(Node[].class);
        if (nodes != null && nodes.length > 0)
        {
            List<Node> newNodes = Arrays.asList(nodes);
            newNodes.removeAll(cluster.getAllNodes());
            for (Node node : newNodes)
            {
                try
                {
                    // Register Node and send HI message
                    cluster.register(node);
                    server.message(node, cluster.messageHI());
                } catch (IOException | MeshMapException e)
                {
                    LOG.log(Level.SEVERE, "Could not register Node: " + e.getMessage(), e);
                }
            }
        }
    }
}