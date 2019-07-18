/**
 * 
 */
package jamsesso.meshmap;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    public DiscoveryMeshMapCluster(Node self, File directory, Node... mesh)
    throws MeshMapException
    {
        this(self, directory);

        if (mesh != null)
        {
            for (Node node : mesh)
            {
                register(node);
            }
        }
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
    public <K, V> MeshMap<K, V> join()
    throws MeshMapException
    {
        if (this.map != null)
        {
            return this.map;
        }
        
        File file = new File(directory.getAbsolutePath() + File.separator + self.toString());
        
        try
        {
            boolean didCreateFile = file.createNewFile();
            
            if (!didCreateFile)
            {
                throw new MeshMapException("File could not be created: " + file.getName());
            }
        } catch (IOException e)
        {
            throw new MeshMapException("Unable to join cluster", e);
        }
        
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
    public void close()
    throws Exception
    {
        if (server != null)
        {
            server.broadcast(Message.BYE);
            server.close();
        }
    }
}
