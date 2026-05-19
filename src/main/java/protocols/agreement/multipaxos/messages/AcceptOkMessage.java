package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import protocols.agreement.multipaxos.auxiliaryClasses.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AcceptOkMessage extends ProtoMessage {
    public final static short MSG_ID = 104;

    private final UUID opId;    //Provavelmente não precisamos de todas essas variáveis
    private final int instance;
    private final byte[] op;
    private final Ballot ballot;

    public AcceptOkMessage(int instance, UUID opId, byte[] op, Ballot ballot) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.ballot = ballot;
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

    @Override
    public String toString() {
        return "AcceptOkMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                ", ballot=" + ballot +
                '}';
    }

    public static ISerializer<AcceptOkMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AcceptOkMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
            Ballot.serialize(msg.ballot, out);
        }

        @Override
        public AcceptOkMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            Ballot ballot = Ballot.deserialize(in);
            return new AcceptOkMessage(instance, opId, op, ballot);
        }
    };

}
