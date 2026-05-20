package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class RequestVoteReplyMsg extends ProtoMessage {
    public static final short MSG_ID = 102;
    private final int term;
    private final boolean voteGranted;

    public RequestVoteReplyMsg(int term, boolean voteGranted) {
        super(MSG_ID);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }

    public static ISerializer<RequestVoteReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(RequestVoteReplyMsg m, ByteBuf out) {
            out.writeInt(m.term);
            out.writeBoolean(m.voteGranted);
        }
        @Override
        public RequestVoteReplyMsg deserialize(ByteBuf in) {
            return new RequestVoteReplyMsg(in.readInt(), in.readBoolean());
        }
    };
}