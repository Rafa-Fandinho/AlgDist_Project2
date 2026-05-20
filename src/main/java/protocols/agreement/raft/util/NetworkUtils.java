package protocols.agreement.raft.util;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NetworkUtils {
    // Utility methods for network packaging of Hosts (Babel Mismatch Prevention)
    public static void serializeHost(Host host, ByteBuf out) {
        if (host == null) { out.writeBoolean(false); }
        else {
            out.writeBoolean(true);
            byte[] ip = host.getAddress().getAddress();
            out.writeInt(ip.length);
            out.writeBytes(ip);
            out.writeInt(host.getPort());
        }
    }

    public static Host deserializeHost(ByteBuf in) throws UnknownHostException {
        if (!in.readBoolean()) return null;
        byte[] ip = new byte[in.readInt()];
        in.readBytes(ip);
        return new Host(InetAddress.getByAddress(ip), in.readInt());
    }
}
