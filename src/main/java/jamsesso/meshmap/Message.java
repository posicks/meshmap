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
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Messages have the following byte format.
 *
 * +-----------------+------------------+----------------+
 * | 32 byte type ID | 4 byte size (=X) | X byte payload |
 * +-----------------+------------------+----------------+
 */
public class Message
{
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
    
    private static final int MESSAGE_TYPE = 32;
    
    private static final int MESSAGE_SIZE = 4;
    
    private final String type;
    
    private final int length;
    
    private final byte[] payload;
    
    
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
        byte[] remainingBytes = new byte[MESSAGE_TYPE - typeBytes.length];
        
        buffer.put(typeBytes);
        buffer.put(remainingBytes);
        buffer.putInt(length);
        buffer.put(payload);
        
        outputStream.write(buffer.array());
    }
    
    
    public static Message read(InputStream inputStream)
    throws IOException
    {
        byte[] msgType = new byte[MESSAGE_TYPE];
        byte[] msgSize = new byte[MESSAGE_SIZE];
        
        inputStream.read(msgType);
        inputStream.read(msgSize);
        
        // Create a buffer for the payload
        int size = ByteBuffer.wrap(msgSize).getInt();
        byte[] msgPayload = new byte[size];
        
        inputStream.read(msgPayload);
        
        return new Message(new String(msgType).trim(), msgPayload);
    }
    
    
    private static byte[] toBytes(Object object)
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
    
    
    private static Object fromBytes(byte[] bytes)
    {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = new ObjectInputStream(bis))
        {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e)
        {
            throw new MeshMapMarshallException(e);
        }
    }
    
    
    private static void checkType(String type)
    {
        if (type == null)
        {
            throw new IllegalArgumentException("Type cannot be null");
        }
        
        if (type.getBytes().length > MESSAGE_TYPE)
        {
            throw new IllegalArgumentException("Type cannot exceed 32 bytes");
        }
    }
    
    
    @java.lang.Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Message))
            return false;
        final Message other = (Message) o;
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

    
    @java.lang.Override
    public int hashCode()
    {
        final int PRIME = 31;
        int result = 1;
        final String type = this.type;
        final int length = this.length;
        final byte[] payload = this.payload;
        result = result * PRIME + (type == null ? 0 : type.hashCode());
        result = result * PRIME + (length ^ (length >>> 16));
        result = result * PRIME + (payload == null ? 0 : Arrays.hashCode(payload));
        return result;
    }
    
    
    @Override
    public String toString()
    {
        return "Message(Type=" + type + ", Length=" + length + ")";
    }
}
