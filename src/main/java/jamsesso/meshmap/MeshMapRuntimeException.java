package jamsesso.meshmap;

public class MeshMapRuntimeException extends RuntimeException
{
    private static final long serialVersionUID = 201907030958L;
    
    
    public MeshMapRuntimeException()
    {
        super();
    }
    
    
    public MeshMapRuntimeException(String msg)
    {
        super(msg);
    }
    
    
    public MeshMapRuntimeException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    
    public MeshMapRuntimeException(Throwable cause)
    {
        super(cause);
    }
}
