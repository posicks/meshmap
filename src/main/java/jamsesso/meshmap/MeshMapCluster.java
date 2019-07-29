package jamsesso.meshmap;
 
import java.util.List;
 
public interface MeshMapCluster extends AutoCloseable
{
    public Node getSelf();
   
    
    public List<Node> getAllNodes();
   
 
    public <K, V> MeshMap<K, V> join()
    throws MeshMapException;
   
    
    public <T> T register(Node node)
    throws MeshMapException;
   
    
    public <T> T unregister(Node node);
   
    
    public Node getNodeForKey(Object key);
   
    
    public Node getSuccessorNode();
   
    
    public Message messageHI();
   
    
    public Message messageBYE();
   
    
    public Message messageACK();
   
    
    public Message messageERR();
   
    
    public Message messageYES();
   
    
    public Message messageNO();
   
    
    public Message messageNOOP();
}