package protocols.agreement.multipaxos.auxiliaryClasses;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Ballot implements Comparable<Ballot>{
    //Basicamente, ter informação do Host garante que haja um critério de desempate caso dois processos usem o mesmo sn
    private int sequenceNumber;
    private final Host leader;

    public Ballot(int sequenceNumber, Host leader) {
        this.sequenceNumber = sequenceNumber;
        this.leader = leader;
    }

    public Host getLeader() {
        return leader;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void updateAndIncrement(Ballot highestSeen) {
        if (highestSeen.getSequenceNumber() >= this.sequenceNumber) {
            this.sequenceNumber = highestSeen.getSequenceNumber() + 1;
        } else {
            this.sequenceNumber++;
        }
    }

    @Override
    public int compareTo(Ballot other) {    //Pedi pro Gemini fazer tbm
        if (this.sequenceNumber != other.sequenceNumber) {
            return Integer.compare(this.sequenceNumber, other.sequenceNumber);
        }
        int ipCompare = this.leader.getAddress().getHostAddress()
                .compareTo(other.leader.getAddress().getHostAddress());
        if (ipCompare != 0) return ipCompare;
        return Integer.compare(this.leader.getPort(), other.leader.getPort());
    }

    @Override
    public String toString() {
        return "(" + sequenceNumber + ", " + leader.getAddress().getHostAddress() + ":" + leader.getPort() + ")";
    }

    public static void serialize(Ballot ballot, ByteBuf out) {
        out.writeInt(ballot.sequenceNumber);
        byte[] ipBytes = ballot.leader.getAddress().getAddress();
        out.writeInt(ipBytes.length);
        out.writeBytes(ipBytes);
        out.writeInt(ballot.leader.getPort());
    }

    public static Ballot deserialize(ByteBuf in) {
        int sn = in.readInt();
        byte[] ipBytes = new byte[in.readInt()];
        in.readBytes(ipBytes);
        int port = in.readInt();
        try {
            InetAddress address = InetAddress.getByAddress(ipBytes);
            Host leaderHost = new Host(address, port);
            return new Ballot(sn, leaderHost);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to deserialize Host in Ballot", e);
        }
    }
}
