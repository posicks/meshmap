package jamsesso.meshmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Messages have the following byte format.
 *
 * +--------------+-----------------+------------------+----------------+
 * | MAGIC NUMBER | 16 byte type ID | 4 byte size (=X) | X byte payload |
 * +--------------+-----------------+------------------+----------------+
 */
public class Message
{
    public static final int MAGIC = ByteBuffer.wrap(new byte[] { 0123, 0124, 0105, 0126 }).getInt();
    
    public static final String TYPE_HI = "HI";
    
    public static final String TYPE_BYE = "BYE";
    
    public static final String TYPE_ACK = "ACK";
    
    public static final String TYPE_ERR = "ERR";
    
    public static final String TYPE_YES = "YES";
    
    public static final String TYPE_NO = "NO";
    
    public static final Message HI = new Message(TYPE_HI);
    
    public static final Message BYE = new Message(TYPE_BYE);
    
    public static final Message ACK = new Message(TYPE_ACK);
    
    public static final Message ERR = new Message(TYPE_ERR);
    
    public static final Message YES = new Message(TYPE_YES);
    
    public static final Message NO = new Message(TYPE_NO);
    
    protected static final int MESSAGE_TYPE = 16;
    
    protected static final int MESSAGE_SIZE = 4;
    
    protected Node node = null;
    
    protected final String type;
    
    protected final int length;
    
    protected final byte[] payload;
    
    
    public Message(String type)
    {
        this(type, new byte[0]);
    }
    
    
    public Message(String type, Object payload)
    {
        this(type, toBytes(payload));
    }
    
    
    public Message(String type, byte[] payload)
    {
        checkType(type);
        this.type = type;
        this.length = payload.length;
        this.payload = payload;
    }
    
    
    public Message assignNode(Node node)
    {
        this.node = node;
        return this;
    }
    
    
    public Node getNode()
    {
        return this.node;
    }
    
    
    public String getType()
    {
        return this.type;
    }
    
    
    public int getLength()
    {
        return this.length;
    }
    
    
    public <T> T getPayload(Class<T> clazz)
    {
        return clazz.cast(fromBytes(payload));
    }
    
    
    public int getPayloadAsInt()
    {
        return ByteBuffer.wrap(payload).getInt();
    }
    
    
    public void write(OutputStream outputStream)
    throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_TYPE + MESSAGE_SIZE + length);
        byte[] typeBytes = type.getBytes();

        int remaining = MESSAGE_TYPE - typeBytes.length;
        if (remaining < 0)
        {
            throw new IOException("Message Type must be " + MESSAGE_TYPE + " bytes or less in size");
        }
        byte[] remainingBytes = new byte[remaining];
        
        buffer.put(BigInteger.valueOf(MAGIC).toByteArray());
        buffer.put(typeBytes);
        buffer.put(remainingBytes);
        buffer.putInt(length);
        buffer.put(payload);
        
        outputStream.write(buffer.array());
    }
    
    
    public static Message read(InputStream inputStream)
    throws IOException
    {
        byte[] msgMagic = new byte[4];
        byte[] msgType = new byte[MESSAGE_TYPE];
        byte[] msgSize = new byte[MESSAGE_SIZE];
        
        inputStream.read(msgMagic);
        BigInteger magic = new BigInteger(msgMagic);
        if (magic.intValue() != MAGIC)
        {
            throw new IOException("Message Magic number \"" + magic + "\" does not match expected value \"" + MAGIC + "\"");
        }
        
        inputStream.read(msgType);
        inputStream.read(msgSize);
        
        // Create a buffer for the payload
        int size = ByteBuffer.wrap(msgSize).getInt();
        byte[] msgPayload = new byte[size];
        
        inputStream.read(msgPayload);
        
        return new Message(new String(msgType).trim(), msgPayload);
    }
    
    
    protected static byte[] toBytes(Object object)
    {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos))
        {
            out.writeObject(object);
            return bos.toByteArray();
        } catch (IOException e)
        {
            throw new MeshMapMarshallException(e);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    protected static <T> T fromBytes(byte[] bytes)
    {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = new ObjectInputStream(bis))
        {
            return (T) in.readObject();
        } catch (IOException | ClassNotFoundException e)
        {
            throw new MeshMapMarshallException(e);
        }
    }
    
    
    protected static void checkType(String type)
    {
        if (type == null)
        {
            throw new IllegalArgumentException("Type cannot be null");
        }
        
        if (type.getBytes().length > MESSAGE_TYPE)
        {
            throw new IllegalArgumentException("Type cannot exceed " + MESSAGE_TYPE + " bytes");
        }
    }
    
    
    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Message))
            return false;
        final Message other = (Message) o;
        final Node thisNode = this.node;
        final Node thatNode = other.node;
        if (thisNode == null ? thatNode != null : !thisNode.equals(thatNode))
            return false;
        final String thisType = this.type;
        final String thatType = other.type;
        if (thisType == null ? thatType != null : !thisType.equals(thatType))
            return false;
        if (other.length != this.length)
            return false;
        final byte[] thisPayload = this.payload;
        final byte[] thatPayload = other.payload;
        if (thisPayload == null ? thatPayload != null : !Arrays.equals(thisPayload, thatPayload))
            return false;
        return true;
    }

    
    @Override
    public int hashCode()
    {
        final int PRIME = 31;
        int result = 1;
        final Node node = this.node;
        final String type = this.type;
        final int length = this.length;
        final byte[] payload = this.payload;
        result = result * PRIME + (node == null ? 0 : node.hashCode());
        result = result * PRIME + (type == null ? 0 : type.hashCode());
        result = result * PRIME + (length ^ (length >>> 16));
        result = result * PRIME + (payload == null ? 0 : Arrays.hashCode(payload));
        return result;
    }
    
    
    @Override
    public String toString()
    {
        return "Message(Node = " + node + ", Type=" + type + ", Length=" + length + ")";
    }
}
