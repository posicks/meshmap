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
 
public class DiscoveryWorkerNode
{
    public static void main(String[] args)
    throws Exception
    {
        // Get input from arguments.
        int port = Integer.parseInt(args[0]);
        String directory = args[1];
 
        // Set up cluster and wait. Enter key kills the server.
        Node self = new Node(new InetSocketAddress("127.0.0.1", port));
        final Set<Node> nodes = new LinkedHashSet<>(args.length - 2);
       
        if (args.length >= 3)
        {
            Arrays.stream(args, 2, args.length).parallel().forEach(arg -> {
                int nodeport = port;
                String nodename = arg;
               
                if (nodename != null && nodename.length() > 0)
                {
                    String[] parts = nodename.split(":");
                    if (parts.length == 2)
                    {
                        nodename = parts[0];
                        nodeport = Integer.parseInt(parts[1]);
                    } else if (parts.length == 1)
                    {
                        nodename = parts[0];
                    } else
                    {
                        throw new IllegalArgumentException("Node name must be in the format \"host[:port]\".");
                    }
                }
               
                nodes.add(new Node(new InetSocketAddress(nodename, nodeport)));
            });
        }
       
        try (MeshMapCluster cluster = new DiscoveryMeshMapCluster(self, new File("cluster/" + directory), nodes); MeshMap<String, String> map = cluster.join())
        {
            System.in.read();
            System.out.println("Node is going down...");
        }
    }
}