/**
*
 */
package jamsesso.meshmap;
 
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
 
/**
* @author Steve Posick
*/
public class DiscoveryMeshMapCluster extends LocalMeshMapCluster
{
    private static Logger LOG = Logger.getLogger(DiscoveryMeshMapCluster.class.getName());
   
    protected Map<Node, File> mesh;
   
    /**
     * @param self
     * @param directory
     */
    public DiscoveryMeshMapCluster(Node self, File directory)
    throws MeshMapException
    {
        super(self, directory);
        this.mesh = new ConcurrentHashMap<>();
       
        if (directory != null && directory.isDirectory())
        {
            File[] files = directory.listFiles();
            if (files != null)
            {
                for (File file : files)
                {
                    register(Node.from(file.getName()));
                }
            } else
            {
                LOG.config("No Node files found. Initial Node in Cluster.");
            }
        } else
        {
            throw new MeshMapException("\"" + directory.getName() + "\" is not a directory. Initial Node in Cluster.");
        }
    }
 
   
    /**
     * @param self
     * @param directory
     * @throws MeshMapException
     */
    public DiscoveryMeshMapCluster(Node self, File directory, Collection<Node> mesh)
    throws MeshMapException
    {
        this(self, directory);
 
        if (mesh != null)
        {
            mesh.stream().forEach(node ->
            {
                try
                {
                    register(node);
                } catch (MeshMapException e)
                {
                    LOG.log(Level.WARNING, "Could not register node \"" + node + "\" with Cluster.", e);
                }
            });
        }
    }
 
   
    /**
     * @param self
     * @param directory
     * @throws MeshMapException
     */
    public DiscoveryMeshMapCluster(Node self, File directory, Node... mesh)
    throws MeshMapException
    {
        this(self, directory, Arrays.asList(mesh));
    }
   
    
    @Override
    @SuppressWarnings("unchecked")
    public File register(Node node)
    throws MeshMapException
    {
        if (node == null)
        {
            throw new MeshMapException("A Node must be provided for cluster registration");
        }
       
        File file = new File(directory.getAbsolutePath() + File.separator + node.toString());
       
        try
        {
            if (!file.exists())
            {
                if (!file.createNewFile())
                {
                    throw new MeshMapException("Unable to join cluster; File could not be created: " + file.getName());
                }
            }
        } catch (SecurityException | IOException e)
        {
            throw new MeshMapException("Unable to join cluster; File could not be created: " + file.getName(), e);
        }
       
        File temp = this.mesh.get(node);
        if (temp != null)
        {
            if (!temp.equals(file))
            {
                file.delete();
                throw new MeshMapException("Unable to join node (" + file.toString() + "#" + file.hashCode() + ") to cluster.  Conflicting Node already registered (" + temp.toString() + "#" + temp.hashCode() + ")");
            }
            return temp;
        }
       
        this.mesh.put(node, file);
        return file;
    }
   
    
    @Override
    @SuppressWarnings("unchecked")
    public File unregister(Node node)
    {
        File file = this.mesh.remove(node);
        if (file != null)
        {
            file.delete();
        }
        return file;
    }
   
    
    @Override
    public Message messageHI()
    {
        return new Message(Message.TYPE_HI, getAllNodes().toArray());
    }
   
    
    @Override
    public Message messageACK()
    {
        return new Message(Message.TYPE_ACK, getAllNodes().toArray());
    }
   
    
    @Override
    public void close()
    throws Exception
    {
        if (server != null)
        {
            server.broadcast(messageBYE());
            server.close();
        }
    }
}