package protocols.agreement.incorrect.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Raft AppendEntries RPC reply (§5.3).
 *
 * {@code matchIndex} carries the follower's last log index after the operation,
 * which the leader uses to update its per-follower matchIndex/nextIndex quickly
 * (avoiding the need for multiple round-trips when nextIndex is far off).
 */
public class AppendEntriesReply extends ProtoMessage {

    public static final short MSG_ID = 204;

    private final int     term;
    private final boolean success;
    /** Last log index of the follower after processing the RPC. */
    private final int     matchIndex;

    public AppendEntriesReply(int term, boolean success, int matchIndex) {
        super(MSG_ID);
        this.term       = term;
        this.success    = success;
        this.matchIndex = matchIndex;
    }

    public int     getTerm()       { return term; }
    public boolean isSuccess()     { return success; }
    public int     getMatchIndex() { return matchIndex; }

    @Override
    public String toString() {
        return "AppendEntriesReply{term=" + term + ", success=" + success
                + ", matchIndex=" + matchIndex + "}";
    }

    public static final ISerializer<AppendEntriesReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(AppendEntriesReply msg, ByteBuf buf) {
            buf.writeInt(msg.term);
            buf.writeBoolean(msg.success);
            buf.writeInt(msg.matchIndex);
        }

        @Override
        public AppendEntriesReply deserialize(ByteBuf buf) {
            int     term       = buf.readInt();
            boolean success    = buf.readBoolean();
            int     matchIndex = buf.readInt();
            return new AppendEntriesReply(term, success, matchIndex);
        }
    };
}
