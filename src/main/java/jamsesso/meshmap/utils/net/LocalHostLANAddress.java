package jamsesso.meshmap.utils.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class LocalHostLANAddress
{
    InetAddress self;
    
    
    public LocalHostLANAddress(InetAddress self)
    throws UnknownHostException
    {
        if (self != null)
        {
            this.self = self;
        }
        self = getLocalHostLANAddress();
    }
    
    
    /**
     * Returns an <code>InetAddress</code> object encapsulating what is most likely the machine's LAN IP address.
     * <p/>
     * This method is intended for use as a replacement of JDK method <code>InetAddress.getLocalHost</code>, because
     * that method is ambiguous on Linux systems. Linux systems enumerate the loopback network interface the same
     * way as regular LAN network interfaces, but the JDK <code>InetAddress.getLocalHost</code> method does not
     * specify the algorithm used to select the address returned under such circumstances, and will often return the
     * loopback address, which is not valid for network communication. Details
     * <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4665037">here</a>.
     * <p/>
     * This method will scan all IP addresses on all network interfaces on the host machine to determine the IP address
     * most likely to be the machine's LAN address. If the machine has multiple IP addresses, this method will prefer
     * a site-local IP address (e.g. 192.168.x.x or 10.10.x.x, usually IPv4) if the machine has one (and will return the
     * first site-local address if the machine has more than one), but if the machine does not hold a site-local
     * address, this method will return simply the first non-loopback address found (IPv4 or IPv6).
     * <p/>
     * If this method cannot find a non-loopback address using this selection algorithm, it will fall back to
     * calling and returning the result of JDK method <code>InetAddress.getLocalHost</code>.
     * <p/>
     *
     * @throws UnknownHostException If the LAN address of the machine cannot be found.
     */
    public static final InetAddress getLocalHostLANAddress()
    throws UnknownHostException
    {
        try
        {
            List<InetAddress> nonVirtualCandidates = new LinkedList<>();
            List<InetAddress> virtualCandidates = new LinkedList<>();
            // Iterate all NICs (network interface cards)...
            for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();)
            {
                NetworkInterface iface = ifaces.nextElement();
                // Iterate all IP addresses assigned to each card...
                for (Enumeration<InetAddress> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();)
                {
                    InetAddress inetAddr = inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress())
                    {                        
                        if (iface.isVirtual())
                        {
                            virtualCandidates.add(inetAddr);
                        } else
                        {
                            nonVirtualCandidates.add(inetAddr);
                        }
                    }
                }
            }
            
            /**
             *  Search for address placing priority on addresses as follows:
             *    1. Site Local Address
             *      a. Non-Virtual
             *      b. Virtual
             */
            Optional<InetAddress> op;
            InetAddress inetAddr;
            op = nonVirtualCandidates.parallelStream().filter(addr -> addr.isSiteLocalAddress()).findFirst();
            if (op.isPresent() && (inetAddr = op.get()) != null)
                return inetAddr;
            op = virtualCandidates.parallelStream().filter(addr -> addr.isSiteLocalAddress()).findFirst();
            if (op.isPresent() && (inetAddr = op.get()) != null)
                return inetAddr;
            op = nonVirtualCandidates.stream().findFirst();
            if (op.isPresent() && (inetAddr = op.get()) != null)
                return inetAddr;
            op = virtualCandidates.stream().findFirst();
            if (op.isPresent() && (inetAddr = op.get()) != null)
                return inetAddr;
            
            // We did could not find a non-loopback address; fall back to InetAddress.getLocalHost()
            if ((inetAddr = InetAddress.getLocalHost()) != null)
            {
                return inetAddr;
            }
            
            throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
        } catch (Exception e)
        {
            UnknownHostException uhe = new UnknownHostException("Failed to determine LAN address: " + e);
            uhe.initCause(e);
            throw uhe;
        }
    }
}
