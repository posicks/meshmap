package jamsesso.meshmap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MeshMapImpl<K, V> implements MeshMap<K, V>, Handler<Message>
{
    protected final CachedMeshMapCluster cluster;
    
    protected final MeshMapServer server;
    
    protected final Node self;
    
    protected final Map<Object, Object> delegate;
    
    
    public MeshMapImpl(MeshMapCluster cluster, MeshMapServer server, Node self)
    {
        this.cluster = new CachedMeshMapCluster(cluster);
        this.server = server;
        this.self = self;
        this.delegate = new ConcurrentHashMap<>();
    }
    
    
    @Override
    public Message handle(Message message)
    {
        switch (message.getType())
        {
            case Message.TYPE_HI:
            case Message.TYPE_BYE:
            {
                cluster.clearCache();
                return Message.ACK;
            }
            case TYPE_GET:
            {
                Object key = message.getPayload(Object.class);
                return new Message(TYPE_GET, delegate.get(key));
            }
            case TYPE_PUT:
            {
                Entry entry = message.getPayload(Entry.class);
                delegate.put(entry.getKey(), entry.getValue());
                return Message.ACK;
            }
            case TYPE_REMOVE:
            {
                Object key = message.getPayload(Object.class);
                return new Message(TYPE_REMOVE, delegate.remove(key));
            }
            case TYPE_CLEAR:
            {
                delegate.clear();
                return Message.ACK;
            }
            case TYPE_KEY_SET:
            {
                Object[] keys = delegate.keySet().toArray();
                return new Message(TYPE_KEY_SET, keys);
            }
            case TYPE_SIZE:
            {
                return new Message(TYPE_SIZE, ByteBuffer.allocate(4).putInt(delegate.size()).array());
            }
            case TYPE_CONTAINS_KEY:
            {
                Object key = message.getPayload(Object.class);
                return delegate.containsKey(key) ? Message.YES : Message.NO;
            }
            case TYPE_CONTAINS_VALUE:
            {
                Object value = message.getPayload(Object.class);
                return delegate.containsValue(value) ? Message.YES : Message.NO;
            }
            case TYPE_DUMP_ENTRIES:
            {
                Entry[] entries = delegate.entrySet().stream().map(entry -> new Entry(entry.getKey(), entry.getValue())).collect(Collectors.toList()).toArray(new Entry[0]);
                
                return new Message(TYPE_DUMP_ENTRIES, entries);
            }
            default:
            {
                return Message.ACK;
            }
        }
    }
    
    
    @Override
    public int size()
    {
        Message sizeMsg = new Message(TYPE_SIZE);
        
        return delegate.size() + server.broadcast(sizeMsg).stream().filter(response -> TYPE_SIZE.equals(response.getType())).mapToInt(Message::getPayloadAsInt).sum();
    }
    
    
    @Override
    public boolean isEmpty()
    {
        return size() == 0;
    }
    
    
    @Override
    public boolean containsKey(Object key)
    {
        Node target = cluster.getNodeForKey(key);
        
        if (target.equals(self))
        {
            // Key lives on the current node.
            return delegate.containsKey(key);
        }
        
        Message containsKeyMsg = new Message(TYPE_CONTAINS_KEY, key);
        Message response;
        
        try
        {
            response = server.message(target, containsKeyMsg);
        } catch (IOException e)
        {
            throw new MeshMapRuntimeException(e);
        }
        
        return Message.YES.getType().equals(response.getType());
    }
    
    
    @Override
    public boolean containsValue(Object value)
    {
        if (delegate.containsValue(value))
        {
            // Check locally first.
            return true;
        }
        
        Message containsValueMsg = new Message(TYPE_CONTAINS_VALUE, value);
        
        return server.broadcast(containsValueMsg).stream().map(Message::getType).anyMatch(Message.YES.getType()::equals);
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key)
    {
        return (V) get(key, cluster.getNodeForKey(key));
    }
    
    
    @Override
    public V put(K key, V value)
    {
        put(key, value, cluster.getNodeForKey(key));
        return value;
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key)
    {
        return (V) remove(key, cluster.getNodeForKey(key));
    }
    
    
    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
        m.entrySet().parallelStream().forEach(entry -> put(entry.getKey(), entry.getValue()));
    }
    
    
    @Override
    public void clear()
    {
        Message clearMsg = new Message(TYPE_CLEAR);
        server.broadcast(clearMsg);
        delegate.clear();
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public Set<K> keySet()
    {
        return cluster.getAllNodes().parallelStream().map(this::keySet).flatMap(Stream::of).map(object -> (K) object).collect(Collectors.toSet());
    }
    
    
    @Override
    public Collection<V> values()
    {
        return entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public Set<Map.Entry<K, V>> entrySet()
    {
        Message dumpEntriesMsg = new Message(TYPE_DUMP_ENTRIES);
        Set<Map.Entry<K, V>> entries = new HashSet<>();
        
        for (Map.Entry<Object, Object> localEntry : delegate.entrySet())
        {
            entries.add(new TypedEntry<>((K) localEntry.getKey(), (V) localEntry.getValue()));
        }
        
        for (Message response : server.broadcast(dumpEntriesMsg))
        {
            Entry[] remoteEntries = response.getPayload(Entry[].class);
            
            for (Entry remoteEntry : remoteEntries)
            {
                entries.add(new TypedEntry<>((K) remoteEntry.getKey(), (V) remoteEntry.getValue()));
            }
        }
        
        return entries;
    }
    
    
    @Override
    public String toString()
    {
        return "MeshMapImpl(Local)[" + String.join(", ", delegate.entrySet().stream().map(entry -> entry.getKey() + ":" + entry.getValue()).collect(Collectors.toList()).toArray(new String[0])) + "]";
    }
    
    
    @SuppressWarnings("unused")
    public void open()
    throws MeshMapException
    {
        Node successor = cluster.getSuccessorNode();
        
        // If there is no successor, there is nothing to do.
        if (successor == null)
        {
            return;
        }
        
        // Ask the successor for their key set.
        Object[] keySet = keySet(successor);
        
        // Transfer the keys from the successor node that should live on this node.
        List<Object> keysToTransfer = Stream.of(keySet).filter(key -> {
            int hash = key.hashCode() & Integer.MAX_VALUE;
            
            if (self.getId() > successor.getId())
            {
                // The successor is the first node (circular node list)
                return hash <= self.getId() && hash > successor.getId();
            }
            
            return hash <= self.getId();
        }).collect(Collectors.toList());
        
        // Store the values on the current node.
        keysToTransfer.forEach(key -> delegate.put(key, get(key, successor)));
        
        // Delete the keys from the remote node now that the keys are transferred.
        keysToTransfer.forEach(key -> remove(key, successor));
    }
    
    
    @Override
    public void close()
    throws Exception
    {
        Node successor = cluster.getSuccessorNode();
        
        // If there is no successor, there is nothing to do.
        if (successor == null)
        {
            return;
        }
        
        // Transfer the data from this node to the successor node.
        delegate.forEach((key, value) -> put(key, value, successor));
    }
    
    
    protected Object get(Object key, Node target)
    {
        if (target.equals(self))
        {
            // Value is stored on the local server.
            return delegate.get(key);
        }
        
        Message getMsg = new Message(TYPE_GET, key);
        Message response;
        
        try
        {
            response = server.message(target, getMsg);
        } catch (IOException e)
        {
            throw new MeshMapRuntimeException(e);
        }
        
        if (!TYPE_GET.equals(response.getType()))
        {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }
        
        return response.getPayload(Object.class);
    }
    
    
    protected Object put(Object key, Object value, Node target)
    {
        if (target.equals(self))
        {
            // Value is stored on the local server.
            return delegate.put(key, value);
        }
        
        Message putMsg = new Message(TYPE_PUT, new Entry(key, value));
        Message response;
        
        try
        {
            response = server.message(target, putMsg);
        } catch (IOException e)
        {
            throw new MeshMapRuntimeException(e);
        }
        
        if (!Message.ACK.getType().equals(response.getType()))
        {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }
        
        return value;
    }
    
    
    protected Object remove(Object key, Node target)
    {
        if (target.equals(self))
        {
            // Value is stored on the local server.
            return delegate.remove(key);
        }
        
        Message removeMsg = new Message(TYPE_REMOVE, key);
        Message response;
        
        try
        {
            response = server.message(target, removeMsg);
        } catch (IOException e)
        {
            throw new MeshMapRuntimeException(e);
        }
        
        if (!TYPE_REMOVE.equals(response.getType()))
        {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }
        
        return response.getPayload(Object.class);
    }
    
    
    protected Object[] keySet(Node target)
    {
        if (target.equals(self))
        {
            // Key is on local server.
            return delegate.keySet().toArray();
        }
        
        Message keySetMsg = new Message(TYPE_KEY_SET);
        
        try
        {
            Message response = server.message(target, keySetMsg);
            return response.getPayload(Object[].class);
        } catch (IOException e)
        {
            throw new MeshMapRuntimeException(e);
        }
    }
    
    
    protected static class Entry implements Serializable
    {
        private static final long serialVersionUID = 201907030957L;

        private Object key;
        
        private Object value;
        
        
        public Entry(Object key, Object value)
        {
            super();
            this.key = key;
            this.value = value;
        }
        
        
        @SuppressWarnings("unchecked")
        public <K> K getKey()
        {
            return (K) this.key;
        }
        
        
        @SuppressWarnings("unchecked")
        public <V> V getValue()
        {
            return (V) this.value;
        }
        
        
        @java.lang.Override
        public boolean equals(Object o)
        {
            if (o == this)
                return true;
            if (!(o instanceof TypedEntry))
                return false;
            @SuppressWarnings("rawtypes")
            final TypedEntry other = (TypedEntry) o;
            final Object thisKey = this.getKey();
            final Object thatKey = other.getKey();
            if (thisKey == null ? thatKey != null : !thisKey.equals(thatKey))
                return false;
            final Object thisValue = this.getValue();
            final Object thatValue = other.getValue();
            if (thisValue == null ? thatValue != null : !thisValue.equals(thatValue))
                return false;
            return true;
        }
        
        
        @java.lang.Override
        public int hashCode()
        {
            final int PRIME = 31;
            int result = 1;
            final Object key = this.getKey();
            final Object value = this.getValue();
            result = result * PRIME + (key == null ? 0 : key.hashCode());
            result = result * PRIME + (value == null ? 0 : value.hashCode());
            return result;
        }
        
        
        @java.lang.Override
        public String toString()
        {
            return "Entry(Key=" + getKey() + "}, Value={" + getValue() + "}";
        }
    }
    
    
    protected static class TypedEntry<K, V> implements Map.Entry<K, V>
    {
        private K key;
        
        private V value;
        
        
        public TypedEntry(K key, V value)
        {
            super();
            this.key = key;
            this.value = value;
        }
        
        
        @Override
        public V setValue(V value)
        {
            throw new UnsupportedOperationException();
        }
        
        
        @Override
        public K getKey()
        {
            return this.key;
        }
        
        
        @Override
        public V getValue()
        {
            return this.value;
        }
        
        
        @java.lang.Override
        public boolean equals(Object o)
        {
            if (o == this)
                return true;
            if (!(o instanceof TypedEntry))
                return false;
            @SuppressWarnings("rawtypes")
            final TypedEntry other = (TypedEntry) o;
            final Object thisKey = this.getKey();
            final Object thatKey = other.getKey();
            if (thisKey == null ? thatKey != null : !thisKey.equals(thatKey))
                return false;
            final Object thisValue = this.getValue();
            final Object thatValue = other.getValue();
            if (thisValue == null ? thatValue != null : !thisValue.equals(thatValue))
                return false;
            return true;
        }
        
        
        @java.lang.Override
        public int hashCode()
        {
            final int PRIME = 31;
            int result = 1;
            final Object key = this.getKey();
            final Object value = this.getValue();
            result = result * PRIME + (key == null ? 0 : key.hashCode());
            result = result * PRIME + (value == null ? 0 : value.hashCode());
            return result;
        }
        
        
        @java.lang.Override
        public String toString()
        {
            return "TypedEntry(Key=" + getKey() + "}, Value={" + getValue() + "}";
        }
    }
}
