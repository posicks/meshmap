package jamsesso.meshmap;
 
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
 
public class MeshMapServer implements Runnable, AutoCloseable
{
    private static final Logger LOG = Logger.getLogger(MeshMapServer.class.getName());
   
    protected final MeshMapCluster cluster;
   
    protected final Node self;
   
    protected Handler<Message> handler;
   
    protected volatile boolean started = false;
   
    protected volatile IOException failure = null;
   
    protected ServerSocket serverSocket;
 
    protected Thread thread;
   
    
    public MeshMapServer(MeshMapCluster cluster, Node self)
    {
        this.cluster = cluster;
        this.self = self;
    }
   
    
    public MeshMapServer start(Handler<Message> handler)
    throws IOException
    {
        if (this.started)
        {
            return this;
        }
       
        this.handler = null;
        this.failure = null;
        this.thread = null;
       
        if (handler == null)
        {
            this.handler = (response) -> { return response; };
        } else
        {
            this.handler = handler;
        }
       
        this.thread = new Thread(new ThreadGroup("MeshMap Threads"), this, "MeshMap Main Thread");
        thread.start();
       
        // Wait for the server to start.
        while (!this.started)
        {
            try
            {
                Thread.sleep(100);
            } catch (InterruptedException e)
            {
                // Do nothing
            }
        }
       
        if (this.failure != null)
        {
            throw this.failure;
        }
       
        return this;
    }
   
    
    @Override
    public void run()
    {
        ServerSocket serverSocket;
       
        try
        {
            this.serverSocket = serverSocket = new ServerSocket(self.getAddress().getPort());
            this.started = true;
 
            while (!serverSocket.isClosed())
            {
                try (Socket socket = serverSocket.accept();
                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream())
                {
                    Message response = handler.handle(Message.read(inputStream));
                   
                    if (response == null)
                    {
                       response = cluster.messageACK();
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
        } catch (IOException e)
        {
            this.failure = e;
        }
       
        if (!this.serverSocket.isClosed())
        {
            try
            {
                this.serverSocket.close();
            } catch (IOException e)
            {
                LOG.log(Level.WARNING, "Unable to close Server Socket", e);
            }
        }
       
        this.started = false;
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
                        message.assignNode(self).write(outputStream);
                        outputStream.flush();
                        return Message.read(inputStream).assignNode(node);
                    }
                }
            }).on(IOException.class).times(3);
        } catch (Exception e)
        {
            throw new IOException(e);
        }
    }
   
    
    public List<Message> broadcast(Message message)
    {
        return cluster.getAllNodes().parallelStream().filter(node -> !node.equals(self)).map(node -> {
            try
            {
                return message(node, message);
            } catch (IOException e)
            {
                LOG.log(Level.SEVERE, "Unable to broadcast message to node: " + node, e);
                return cluster.messageERR();
            }
        }).collect(Collectors.toList());
    }
   
    
    public void stop()
    {
        this.started = false;
        this.failure = null;
    }
   
    
    @Override
    public void close()
    {
        try
        {
            serverSocket.close();
        } catch (IOException e)
        {
            LOG.log(Level.WARNING, "Error closing Server Socket", e);
        } finally
        {
            started = false;
        }
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
        final Object thisMessageHandler = this.handler;
        final Object thatMessageHandler = other.handler;
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
        final Object messageHandler = this.handler;
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
}