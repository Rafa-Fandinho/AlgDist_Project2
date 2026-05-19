package protocols.agreement.incorrect.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Raft AppendEntries RPC (§5.3).
 * Also serves as the heartbeat when {@code entries} is empty.
 */
public class AppendEntriesMessage extends ProtoMessage {

    public static final short MSG_ID = 203;

    // ── Fields ─────────────────────────────────────────────────────────────
    private final int        term;
    private final Host       leaderId;
    private final int        prevLogIndex;
    private final int        prevLogTerm;
    private final List<Entry> entries;
    private final int        leaderCommit;

    // ── Inner entry class ──────────────────────────────────────────────────
    public static class Entry {
        public final int    term;
        public final UUID   opId;
        public final byte[] op;

        public Entry(int term, UUID opId, byte[] op) {
            this.term = term;
            this.opId = opId;
            this.op   = op;
        }
    }

    // ── Constructor ────────────────────────────────────────────────────────
    public AppendEntriesMessage(int term, Host leaderId, int prevLogIndex, int prevLogTerm,
                                List<Entry> entries, int leaderCommit) {
        super(MSG_ID);
        this.term         = term;
        this.leaderId     = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm  = prevLogTerm;
        this.entries      = entries;
        this.leaderCommit = leaderCommit;
    }

    // ── Accessors ──────────────────────────────────────────────────────────
    public int         getTerm()         { return term; }
    public Host        getLeaderId()     { return leaderId; }
    public int         getPrevLogIndex() { return prevLogIndex; }
    public int         getPrevLogTerm()  { return prevLogTerm; }
    public List<Entry> getEntries()      { return entries; }
    public int         getLeaderCommit() { return leaderCommit; }

    @Override
    public String toString() {
        return "AppendEntriesMessage{term=" + term + ", prevLogIndex=" + prevLogIndex
                + ", entries=" + entries.size() + ", leaderCommit=" + leaderCommit + "}";
    }

    // ── Serializer ─────────────────────────────────────────────────────────
    public static final ISerializer<AppendEntriesMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AppendEntriesMessage msg, ByteBuf buf) throws IOException {
            buf.writeInt(msg.term);
            Host.serializer.serialize(msg.leaderId, buf);
            buf.writeInt(msg.prevLogIndex);
            buf.writeInt(msg.prevLogTerm);
            buf.writeInt(msg.leaderCommit);

            buf.writeInt(msg.entries.size());
            for (Entry e : msg.entries) {
                buf.writeInt(e.term);
                // UUID
                buf.writeLong(e.opId.getMostSignificantBits());
                buf.writeLong(e.opId.getLeastSignificantBits());
                // op bytes
                buf.writeInt(e.op.length);
                buf.writeBytes(e.op);
            }
        }

        @Override
        public AppendEntriesMessage deserialize(ByteBuf buf) throws IOException {
            int  term         = buf.readInt();
            Host leaderId     = Host.serializer.deserialize(buf);
            int  prevLogIndex = buf.readInt();
            int  prevLogTerm  = buf.readInt();
            int  leaderCommit = buf.readInt();

            int count = buf.readInt();
            List<Entry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int    eTerm  = buf.readInt();
                UUID   opId   = new UUID(buf.readLong(), buf.readLong());
                int    opLen  = buf.readInt();
                byte[] op     = new byte[opLen];
                buf.readBytes(op);
                entries.add(new Entry(eTerm, opId, op));
            }

            return new AppendEntriesMessage(term, leaderId, prevLogIndex, prevLogTerm,
                    entries, leaderCommit);
        }
    };
}
