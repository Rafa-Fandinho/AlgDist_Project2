package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class ForwardProposalMessage extends ProtoMessage {
    public final static short MSG_ID = 106;

    private final UUID opId;
    private final byte[] op;
    private final boolean isMembership;

    public ForwardProposalMessage(UUID opId, byte[] op, boolean isMembership) {
        super(MSG_ID);
        this.op = op;
        this.opId = opId;
        this.isMembership = isMembership;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }

    public boolean isMembership() {
        return isMembership;
    }

    @Override
    public String toString() {
        return "ForwardProposalMessage{" +
                "opId=" + opId +
                ", op=" + Hex.encodeHexString(op) +
                ", isMembership=" + isMembership +
                '}';
    }

    public static ISerializer<ForwardProposalMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ForwardProposalMessage msg, ByteBuf out) {
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeBoolean(msg.isMembership);
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
        }

        @Override
        public ForwardProposalMessage deserialize(ByteBuf in) {
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            boolean isMembership = in.readBoolean();
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new ForwardProposalMessage(opId, op, isMembership);
        }
    };
}