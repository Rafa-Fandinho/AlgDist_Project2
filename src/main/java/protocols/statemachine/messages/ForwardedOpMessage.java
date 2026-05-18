package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.util.UUID;

public class ForwardedOpMessage extends ProtoMessage {
    public static final short MSG_ID = 201;
    private final UUID opId;
    private final byte[] operation;

    public ForwardedOpMessage(UUID opId, byte[] operation) {
        super(MSG_ID);
        this.opId = opId;
        this.operation = operation;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    public static final ISerializer<ForwardedOpMessage> serializer = new ISerializer<ForwardedOpMessage>() {
        @Override
        public void serialize(ForwardedOpMessage msg, ByteBuf out) {
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.operation.length);
            out.writeBytes(msg.operation);
        }

        @Override
        public ForwardedOpMessage deserialize(ByteBuf in) {
            UUID id = new UUID(in.readLong(), in.readLong());
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            return new ForwardedOpMessage(id, op);
        }
    };
}