package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import protocols.agreement.multipaxos.auxiliaryClasses.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class DecideMessage extends ProtoMessage {

    public final static short MSG_ID = 105;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final Ballot ballot;
    private final boolean isMembership;

    public DecideMessage(int instance, UUID opId, byte[] op, Ballot ballot, boolean isMembership) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.ballot = ballot;
        this.isMembership = isMembership;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }

    public Ballot getBallot() { return ballot; }

    public boolean isMembership() {
        return isMembership;
    }

    @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                ", ballot=" + ballot +
                ", isMembership=" + isMembership +
                '}';
    }

    public static ISerializer<DecideMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(DecideMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
            Ballot.serialize(msg.ballot, out);
            out.writeBoolean(msg.isMembership);
        }

        @Override
        public DecideMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            Ballot ballot = Ballot.deserialize(in);
            boolean isMembership = in.readBoolean();
            return new DecideMessage(instance, opId, op, ballot, isMembership);
        }
    };

}
