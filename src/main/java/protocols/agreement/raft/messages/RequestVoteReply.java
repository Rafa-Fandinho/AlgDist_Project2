package protocols.agreement.incorrect.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Raft RequestVote RPC reply (§5.2).
 */
public class RequestVoteReply extends ProtoMessage {

    public static final short MSG_ID = 202;

    private final int     term;
    private final boolean granted;

    public RequestVoteReply(int term, boolean granted) {
        super(MSG_ID);
        this.term    = term;
        this.granted = granted;
    }

    public int     getTerm()    { return term; }
    public boolean isGranted()  { return granted; }

    @Override
    public String toString() {
        return "RequestVoteReply{term=" + term + ", granted=" + granted + "}";
    }

    public static final ISerializer<RequestVoteReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(RequestVoteReply msg, ByteBuf buf) {
            buf.writeInt(msg.term);
            buf.writeBoolean(msg.granted);
        }

        @Override
        public RequestVoteReply deserialize(ByteBuf buf) {
            int     term    = buf.readInt();
            boolean granted = buf.readBoolean();
            return new RequestVoteReply(term, granted);
        }
    };
}
