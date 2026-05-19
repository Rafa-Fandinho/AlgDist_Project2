package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class HeartbeatMessage extends ProtoMessage {
    public final static short MSG_ID = 107;

    private final int instance;

    public HeartbeatMessage(int instance) {
        super(MSG_ID);
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return "PrepareMessage{" +
                "instance=" + instance +
                '}';
    }

    public static ISerializer<HeartbeatMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(HeartbeatMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
        }

        @Override
        public HeartbeatMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            return new HeartbeatMessage(instance);
        }
    };

}