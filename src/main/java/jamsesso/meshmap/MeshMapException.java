package jamsesso.meshmap;

public class MeshMapException extends Exception
{
    
    private static final long serialVersionUID = 201901030952L;
    
    
    public MeshMapException()
    {
        super();
    }
    
    
    public MeshMapException(String msg)
    {
        super(msg);
    }
    
    
    public MeshMapException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    
    public MeshMapException(Throwable cause)
    {
        super(cause);
    }
}
