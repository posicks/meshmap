package jamsesso.meshmap;

public class MeshMapMarshallException extends MeshMapRuntimeException
{
    private static final long serialVersionUID = 201907030958L;
    
    
    public MeshMapMarshallException()
    {
        super();
    }
    
    
    public MeshMapMarshallException(String msg)
    {
        super(msg);
    }
    
    
    public MeshMapMarshallException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    
    public MeshMapMarshallException(Throwable cause)
    {
        super(cause);
    }
}
