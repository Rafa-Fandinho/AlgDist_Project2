package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multipaxos.auxiliaryClasses.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {

    public final static short MSG_ID = 102;

    private final int instance;
    private final Ballot ballot;

    public PrepareMessage(int instance, Ballot ballot) {
        super(MSG_ID);
        this.instance = instance;
        this.ballot = ballot;
    }

    public int getInstance() {
        return instance;
    }

    public Ballot getBallot() {
        return ballot;
    }

    @Override
    public String toString() {
        return "PrepareMessage{" +
                "instance=" + instance +
                ", ballot=" + ballot +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            Ballot.serialize(msg.ballot, out);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            Ballot ballot = Ballot.deserialize(in);
            return new PrepareMessage(instance, ballot);
        }
    };
}