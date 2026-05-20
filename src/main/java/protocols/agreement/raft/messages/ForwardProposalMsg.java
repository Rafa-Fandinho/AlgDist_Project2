package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import java.io.IOException;
import java.util.UUID;

public class ForwardProposalMsg extends ProtoMessage {
    public static final short MSG_ID = 105; // Choose a unique ID not in use

    private final int instance;
    private final UUID opId; // Confirm if your opId is UUID or another type
    private final byte[] operation;

    public ForwardProposalMsg(int instance, UUID opId, byte[] operation) {
        super(MSG_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
    }

    public int getInstance() { return instance; }
    public UUID getOpId() { return opId; }
    public byte[] getOperation() { return operation; }

    // -------------------------------------------------------------------------
    // SERIALIZER IMPLEMENTATION (Babel / Netty ByteBuf)
    // -------------------------------------------------------------------------
    public static final ISerializer<ForwardProposalMsg> serializer = new ISerializer<ForwardProposalMsg>() {

        @Override
        public void serialize(ForwardProposalMsg msg, ByteBuf out) throws IOException {
            // 1. Write the instance (int -> 4 bytes)
            out.writeInt(msg.instance);

            // 2. Write the UUID (As UUID is 128 bits, write as 2 64-bit longs)
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());

            // 3. Write the operation byte array (first length, then actual bytes)
            out.writeInt(msg.operation.length);
            out.writeBytes(msg.operation);
        }

        @Override
        public ForwardProposalMsg deserialize(ByteBuf in) throws IOException {
            // 1. Read back the instance
            int instance = in.readInt();

            // 2. Read UUID bits and reconstruct object
            long mostSig = in.readLong();
            long leastSig = in.readLong();
            UUID opId = new UUID(mostSig, leastSig);

            // 3. Read byte array length and the array itself
            int opLength = in.readInt();
            byte[] operation = new byte[opLength];
            in.readBytes(operation);

            // Return a new instance reconstructed from network data
            return new ForwardProposalMsg(instance, opId, operation);
        }
    };
}