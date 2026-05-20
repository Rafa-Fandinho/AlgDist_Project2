package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.raft.util.NetworkUtils;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import java.io.IOException;

public class RequestVoteMsg extends ProtoMessage {
    public static final short MSG_ID = 101;
    private final int term;
    private final Host candidate;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteMsg(int term, Host candidate, int lastLogIndex, int lastLogTerm) {
        super(MSG_ID);
        this.term = term;
        this.candidate = candidate;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() { return term; }
    public Host getCandidate() { return candidate; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }

    public static ISerializer<RequestVoteMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(RequestVoteMsg m, ByteBuf out) throws IOException {
            out.writeInt(m.term);
            NetworkUtils.serializeHost(m.candidate, out);
            out.writeInt(m.lastLogIndex);
            out.writeInt(m.lastLogTerm);
        }
        @Override
        public RequestVoteMsg deserialize(ByteBuf in) throws IOException {
            return new RequestVoteMsg(in.readInt(), NetworkUtils.deserializeHost(in), in.readInt(), in.readInt());
        }
    };
}