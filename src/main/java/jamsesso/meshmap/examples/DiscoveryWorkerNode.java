package jamsesso.meshmap.examples;
 
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
 
import jamsesso.meshmap.DiscoveryMeshMapCluster;
import jamsesso.meshmap.MeshMap;
import jamsesso.meshmap.MeshMapCluster;
import jamsesso.meshmap.Node;
import jamsesso.meshmap.utils.net.LocalHostLANAddress;
 
public class DiscoveryWorkerNode
{
    private static final int DEFAULT_PORT = 5432;


    public static void main(String[] args)
    throws Exception
    {
        try
        {
            Node self = new Node(parseNodeStr(args[0]));
            
            final Set<Node> nodes = new LinkedHashSet<>(args.length - 1);
           
            if (args.length >= 3)
            {
                Arrays.stream(args, 2, args.length).parallel().forEach(arg -> {
                    nodes.add(new Node(parseNodeStr(arg)));
                });
            }
           
            String directory = args[1];
            
            // Set up cluster and wait. Enter key kills the server.
            try (MeshMapCluster cluster = new DiscoveryMeshMapCluster(self, new File(directory), nodes); MeshMap<String, String> map = cluster.join())
            {
                System.in.read();
                System.out.println("Node is going down...");
            }
        } catch (Exception e)
        {
            System.out.printf("Usage \"%s\" : \n\n%s self directory node [[node] ...]\n    self : ipaddress[:port]\n    directory: ./cluster\n    node : ipaddress[:port]\n\nExample: \n    %2$s %s ~/cluster/ %s %s %s", "DiscoveryWorkerNode", "java -jar DiscoveryWorkerNode.jar", LocalHostLANAddress.getLocalHostLANAddress().getHostAddress(), "10.1.1.10", "10.1.1.20:1234", "10.1.1.20:2345");
        }
    }
    
    
    private static final InetSocketAddress parseNodeStr(String node)
    {
        String nodename = "127.0.0.1";
        int nodeport = DEFAULT_PORT;
        
        if (node != null && node.length() > 0)
        {
            String[] parts = node.split(":");
            if (parts.length == 2)
            {
                nodename = parts[0];
                nodeport = Integer.parseInt(parts[1]);
            } else if (parts.length == 1)
            {
                try
                {
                    nodeport = Integer.parseInt(node);
                } catch (NumberFormatException e1)
                {
                    nodename = parts[0];
                }
            } else
            {
                throw new IllegalArgumentException("Node name must be in the format \"host[:port]\".");
            }
        }
        
        return new InetSocketAddress(nodename, nodeport);
    }
}