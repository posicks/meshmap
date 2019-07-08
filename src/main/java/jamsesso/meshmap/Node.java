package jamsesso.meshmap;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;

public class Node implements Serializable
{
    private static final long serialVersionUID = 201907031000L;

    private UUID id = UUID.randomUUID();
    
    private InetSocketAddress address;
    
    
    public Node(InetSocketAddress address)
    {
        this(UUID.randomUUID(), address);
    }
    
    
    public Node(UUID id, InetSocketAddress address)
    {
        this.id = id;
        this.address = address;
    }
    
    
    public int getId()
    {
        return id.hashCode() & Integer.MAX_VALUE;
    }
    
    
    public InetSocketAddress getAddress()
    {
        return this.address;
    }
    
    
    public static Node from(String str)
    {
        if (str == null)
        {
            throw new IllegalArgumentException("String must not be null");
        }
        
        String[] parts = str.split("#");
        
        if (parts.length != 3)
        {
            throw new IllegalArgumentException("Node address must contain only a host and port");
        }
        
        String host = parts[0];
        int port;
        UUID id;
        
        try
        {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Node address port must be a valid number", e);
        }
        
        try
        {
            id = UUID.fromString(parts[2]);
        } catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Node ID must be a valid UUID", e);
        }
        
        return new Node(id, new InetSocketAddress(host, port));
    }
    
    
    @java.lang.Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Node))
            return false;
        final Node other = (Node) o;
        final UUID thisId = this.id;
        final UUID thatId = other.id;
        if (thisId == null ? thatId != null : !thisId.equals(thatId))
            return false;
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
        final Object uuid = this.id;
        final Object address = this.address;
        result = result * PRIME + (uuid == null ? 0 : uuid.hashCode());
        result = result * PRIME + (address == null ? 0 : address.hashCode());
        return result;
    }
    
    
    @Override
    public String toString()
    {
        return address.getHostString() + '#' + address.getPort() + '#' + id;
    }
}
