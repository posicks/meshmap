package jamsesso.meshmap;
 
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
 
public class LocalMeshMapCluster extends AbstractMeshMapCluster
{
    protected final File directory;
   
    protected List<Node> cachedNodes = null;
   
    protected long cachedAt = 0L;
   
    protected long cacheFor = 60000L;
   
    
    public LocalMeshMapCluster(final Node self, final File directory)
    {
        super(self);
       
        this.directory = directory;
        directory.mkdirs();
        if (!directory.isDirectory())
        {
            throw new IllegalArgumentException("File passed to LocalMeshMapCluster must be a directory");
        }
        if (!directory.canRead() || !directory.canWrite())
        {
            throw new IllegalArgumentException("Directory must be readable and writable");
        }
    }
   
    
    @Override
    public List<Node> getAllNodes()
    {
        if (cachedNodes == null || (cachedAt + cacheFor) <= System.currentTimeMillis())
        {
            cachedNodes = Stream.of(directory.listFiles()).filter(File::isFile).map(File::getName).map(Node::from).sorted(Comparator.comparingInt(Node::getId)).collect(Collectors.toList());
            cachedAt = System.currentTimeMillis();
        }
       
        return cachedNodes;
    }
   
    
    @Override
    public void close()
    throws Exception
    {
        File file = new File(directory.getAbsolutePath() + File.separator + self.toString());
        boolean didDeleteFile = file.delete();
       
        if (!didDeleteFile)
        {
            throw new MeshMapException("File could not be deleted: " + file.getName());
        }
       
        super.close();
    }
 
 
    @Override
    @SuppressWarnings("unchecked")
    public File register(Node node)
    throws MeshMapException
    {
        File file = new File(directory.getAbsolutePath() + File.separator + node.toString());
       
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
 
        cachedAt = 0L;
        return file.exists() ? file : null;
    }
 
 
    @Override
    @SuppressWarnings("unchecked")
    public File unregister(Node node)
    {
        File file = new File(directory.getAbsolutePath() + File.separator + node.toString());
       
        cachedAt = 0L;
        return file.delete() ? file : null;
    }
   
    
    @Override
    public <K, V> MeshMap<K, V> join()
    throws MeshMapException
    {
        cachedAt = 0L;
        return super.join();
    }
}
