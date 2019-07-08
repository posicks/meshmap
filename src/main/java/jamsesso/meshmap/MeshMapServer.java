package jamsesso.meshmap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MeshMapServer implements Runnable, AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(MeshMapServer.class.getName());
    
    private final MeshMapCluster cluster;
    
    private final Node self;
    
    private MessageHandler messageHandler;
    
    private volatile boolean started = false;
    
    private volatile IOException failure = null;
    
    private ServerSocket serverSocket;
    
    
    public MeshMapServer(MeshMapCluster cluster, Node self)
    {
        this.cluster = cluster;
        this.self = self;
    }
    
    
    public void start(MessageHandler messageHandler)
    throws IOException
    {
        if (this.messageHandler != null)
        {
            throw new IllegalStateException("Cannot restart a dead mesh map server");
        }
        
        this.messageHandler = messageHandler;
        new Thread(this).start();
        
        // Wait for the server to start.
        while (!started)
        {
            try
            {
                Thread.sleep(100);
            } catch (InterruptedException e)
            {
                // Do nothing
            }
        }
        
        if (failure != null)
        {
            throw failure;
        }
    }
    
    
    public Message message(Node node, Message message)
    throws IOException
    {
        try
        {
            return Retryable.retry(() -> {
                try (Socket socket = new Socket())
                {
                    socket.connect(node.getAddress());
                    
                    try (OutputStream outputStream = socket.getOutputStream();
                    InputStream inputStream = socket.getInputStream())
                    {
                        message.write(outputStream);
                        outputStream.flush();
                        return Message.read(inputStream);
                    }
                }
            }).on(IOException.class).times(3);
        } catch (Exception e)
        {
            throw new IOException(e);
        }
    }
    
    
    public Map<Node, Message> broadcast(Message message)
    {
        return cluster.getAllNodes().parallelStream().filter(node -> !node.equals(self)).map(node -> {
            try
            {
                return new BroadcastResponse(node, message(node, message));
            } catch (IOException e)
            {
                LOG.log(Level.SEVERE, "Unable to broadcast message to node: " + node, e);
                return new BroadcastResponse(node, Message.ERR);
            }
        }).collect(Collectors.toMap(BroadcastResponse::getNode, BroadcastResponse::getResponse));
    }
    
    
    @Override
    public void run()
    {
        try
        {
            serverSocket = new ServerSocket(self.getAddress().getPort());
        } catch (IOException e)
        {
            failure = e;
        } finally
        {
            started = true;
        }
        
        while (!serverSocket.isClosed())
        {
            try (Socket socket = serverSocket.accept();
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream())
            {
                Message message = Message.read(inputStream);
                Message response = messageHandler.handle(message);
                
                if (response == null)
                {
                    response = Message.ACK;
                }
                
                response.write(outputStream);
                outputStream.flush();
            } catch (SocketException e)
            {
                // Socket was closed. Nothing to do here. Node is going down.
            } catch (IOException e)
            {
                LOG.log(Level.SEVERE, "Unable to accept connection", e);
            }
        }
    }
    
    
    @Override
    public void close()
    throws Exception
    {
        serverSocket.close();
    }
    
    
    @java.lang.Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof MeshMapServer))
            return false;
        final MeshMapServer other = (MeshMapServer) o;
        final Object thisCluster = this.cluster;
        final Object thatCluster = other.cluster;
        final Object thisSelf = this.self;
        final Object thatSelf = other.self;
        final Object thisMessageHandler = this.messageHandler;
        final Object thatMessageHandler = other.messageHandler;
        final Object thisServerSocket = this.serverSocket;
        final Object thatServerSocket = other.serverSocket;
        if (thisCluster == null ? thatCluster != null : !thisCluster.equals(thatCluster))
            return false;
        if (thisSelf == null ? thatSelf != null : !thisSelf.equals(thatSelf))
            return false;
        if (thisMessageHandler == null ? thatMessageHandler != null : !thisMessageHandler.equals(thatMessageHandler))
            return false;
        if (thisServerSocket == null ? thatServerSocket != null : !thisServerSocket.equals(thatServerSocket))
            return false;
        return true;
    }
    
    
    @java.lang.Override
    public int hashCode()
    {
        final int PRIME = 31;
        int result = 1;
        final Object cluster = this.cluster;
        final Object self = this.self;
        final Object messageHandler = this.messageHandler;
        final Object serverSocket = this.serverSocket;
        result = result * PRIME + (cluster == null ? 0 : cluster.hashCode());
        result = result * PRIME + (self == null ? 0 : self.hashCode());
        result = result * PRIME + (messageHandler == null ? 0 : messageHandler.hashCode());
        result = result * PRIME + (serverSocket == null ? 0 : serverSocket.hashCode());
        return result;
    }
    
    
    @java.lang.Override
    public String toString()
    {
        return "MeshMapServer(Self={" + self + "}, Cluster={" + cluster + "}, ServerSocket={" + serverSocket + "}, Started=" + started + (failure == null ? "" : ", Failure={" + failure + "}");
    }
    
    
    // @Value
    private static final class BroadcastResponse
    {
        private Node node;
        
        private Message response;
        
        
        @java.beans.ConstructorProperties({"node",
                                           "response"})
        public BroadcastResponse(Node node, Message response)
        {
            this.node = node;
            this.response = response;
        }
        
        
        public Node getNode()
        {
            return this.node;
        }
        
        
        public Message getResponse()
        {
            return response;
        }
        
        
        @java.lang.Override
        public boolean equals(Object o)
        {
            if (o == this)
                return true;
            if (!(o instanceof BroadcastResponse))
                return false;
            final BroadcastResponse other = (BroadcastResponse) o;
            final Object thisNode = this.getNode();
            final Object thatNode = other.getNode();
            if (thisNode == null ? thatNode != null : !thisNode.equals(thatNode))
                return false;
            final Object thisResponse = this.getResponse();
            final Object thatResponse = other.getResponse();
            if (thisResponse == null ? thatResponse != null : !thisResponse.equals(thatResponse))
                return false;
            return true;
        }
        
        
        @java.lang.Override
        public int hashCode()
        {
            final int PRIME = 31;
            int result = 1;
            final Object node = this.getNode();
            final Object response = this.getNode();
            result = result * PRIME + (node == null ? 0 : node.hashCode());
            result = result * PRIME + (response == null ? 0 : response.hashCode());
            return result;
        }
        
        
        @java.lang.Override
        public String toString()
        {
            return "BroadcastResponse(Node={" + getNode() + "}, response={" + getResponse() + "}";
        }
    }
}
