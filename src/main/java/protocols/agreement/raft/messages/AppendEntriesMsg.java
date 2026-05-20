package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.raft.util.LogEntry;
import protocols.agreement.raft.util.NetworkUtils;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AppendEntriesMsg extends ProtoMessage {
    public static final short MSG_ID = 103;
    private final int term;
    private final Host leader;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final int leaderCommit;
    private final List<LogEntry> entries;

    public AppendEntriesMsg(int term, Host leader, int prevLogIndex, int prevLogTerm, int leaderCommit, List<LogEntry> entries) {
        super(MSG_ID);
        this.term = term;
        this.leader = leader;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }

    public int getTerm() { return term; }
    public Host getLeader() { return leader; }
    public int getPrevLogIndex() { return prevLogIndex; }
    public int getPrevLogTerm() { return prevLogTerm; }
    public int getLeaderCommit() { return leaderCommit; }
    public List<LogEntry> getEntries() { return entries; }

    public static ISerializer<AppendEntriesMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(AppendEntriesMsg m, ByteBuf out) throws IOException {
            out.writeInt(m.term);
            NetworkUtils.serializeHost(m.leader, out);
            out.writeInt(m.prevLogIndex);
            out.writeInt(m.prevLogTerm);
            out.writeInt(m.leaderCommit);
            out.writeInt(m.entries.size());
            for (LogEntry e : m.entries) {
                out.writeInt(e.getTerm());
                out.writeInt(e.getIndex());
                out.writeLong(e.getOpId().getMostSignificantBits());
                out.writeLong(e.getOpId().getLeastSignificantBits());
                out.writeInt(e.getOperation().length);
                out.writeBytes(e.getOperation());
            }
        }
        @Override
        public AppendEntriesMsg deserialize(ByteBuf in) throws IOException {
            int term = in.readInt();
            Host leader = NetworkUtils.deserializeHost(in);
            int prevLogIndex = in.readInt();
            int prevLogTerm = in.readInt();
            int leaderCommit = in.readInt();
            int size = in.readInt();
            List<LogEntry> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                entries.add(new LogEntry(in.readInt(), in.readInt(), new UUID(in.readLong(), in.readLong()), new byte[in.readInt()]));
                in.readBytes(entries.get(i).getOperation());
            }
            return new AppendEntriesMsg(term, leader, prevLogIndex, prevLogTerm, leaderCommit, entries);
        }
    };
}