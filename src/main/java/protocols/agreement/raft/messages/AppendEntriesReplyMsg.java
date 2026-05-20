package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.raft.util.NetworkUtils;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import java.io.IOException;

public class AppendEntriesReplyMsg extends ProtoMessage {
    public static final short MSG_ID = 104;
    private final int term;
    private final boolean success;
    private final int matchIndex;
    private final Host from;

    public AppendEntriesReplyMsg(int term, boolean success, int matchIndex, Host from) {
        super(MSG_ID);
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
        this.from = from;
    }

    public int getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public int getMatchIndex() { return matchIndex; }
    public Host getFrom() { return from; }

    public static ISerializer<AppendEntriesReplyMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(AppendEntriesReplyMsg m, ByteBuf out) throws IOException {
            out.writeInt(m.term);
            out.writeBoolean(m.success);
            out.writeInt(m.matchIndex);
            NetworkUtils.serializeHost(m.from, out);
        }
        @Override
        public AppendEntriesReplyMsg deserialize(ByteBuf in) throws IOException {
            return new AppendEntriesReplyMsg(in.readInt(), in.readBoolean(), in.readInt(), NetworkUtils.deserializeHost(in));
        }
    };
}