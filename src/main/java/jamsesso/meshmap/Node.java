package jamsesso.meshmap;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.CharBuffer;

public class Node implements Serializable
{
    private static final long serialVersionUID = 201907031000L;

    private InetSocketAddress address;
    
    
    public Node(InetSocketAddress address)
    {
        this.address = address;
    }
    
    
    public int getId()
    {
        return address.hashCode() & Integer.MAX_VALUE;
    }
    
    
    public InetSocketAddress getAddress()
    {
        return this.address;
    }
    
    
    public void to(Writer out)
    throws IOException
    {
        out.write(address.getHostString() + ':' + address.getPort());
    }
    
    
    public static Node from(Reader in)
    throws IOException
    {
        CharBuffer buffer = CharBuffer.allocate(1024);
        while (in.read(buffer) >= 0);
        return Node.from(buffer.toString());
    }
    
    
    public static Node from(String str)
    {
        if (str == null)
        {
            throw new IllegalArgumentException("String must not be null");
        }
        
        String[] parts = str.split(":");
        
        if (parts.length != 2)
        {
            throw new IllegalArgumentException("Node address must contain only a host and port");
        }
        
        String host = parts[0];
        int port;
        
        try
        {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Node address port must be a valid number", e);
        }
        
        return new Node(new InetSocketAddress(host, port));
    }
    
    
    @java.lang.Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Node))
            return false;
        final Node other = (Node) o;
        final Object thisAddress = this.address;
        final Object thatAddress = other.address;
        if (thisAddress == null ? thatAddress != null : !thisAddress.equals(thatAddress))
            return false;
        return true;
    }
    
    
    @java.lang.Override
    public int hashCode()
    {
        final int PRIME = 31;
        int result = 1;
        final Object address = this.address;
        result = result * PRIME + (address == null ? 0 : address.hashCode());
        return result;
    }
    
    
    @Override
    public String toString()
    {
        return address.getHostString() + ':' + address.getPort();
    }
}
