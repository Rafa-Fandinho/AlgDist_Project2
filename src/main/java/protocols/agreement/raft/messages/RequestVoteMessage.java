package protocols.agreement.incorrect.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

/**
 * Raft RequestVote RPC (§5.2).
 * Sent by candidates to gather votes.
 */
public class RequestVoteMessage extends ProtoMessage {

    public static final short MSG_ID = 201;

    private final int  term;
    private final Host candidateId;
    private final int  lastLogIndex;
    private final int  lastLogTerm;

    public RequestVoteMessage(int term, Host candidateId, int lastLogIndex, int lastLogTerm) {
        super(MSG_ID);
        this.term         = term;
        this.candidateId  = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm  = lastLogTerm;
    }

    public int  getTerm()         { return term; }
    public Host getCandidateId()  { return candidateId; }
    public int  getLastLogIndex() { return lastLogIndex; }
    public int  getLastLogTerm()  { return lastLogTerm; }

    @Override
    public String toString() {
        return "RequestVoteMessage{term=" + term + ", candidate=" + candidateId
                + ", lastLogIndex=" + lastLogIndex + ", lastLogTerm=" + lastLogTerm + "}";
    }

    public static final ISerializer<RequestVoteMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RequestVoteMessage msg, ByteBuf buf) throws IOException {
            buf.writeInt(msg.term);
            Host.serializer.serialize(msg.candidateId, buf);
            buf.writeInt(msg.lastLogIndex);
            buf.writeInt(msg.lastLogTerm);
        }

        @Override
        public RequestVoteMessage deserialize(ByteBuf buf) throws IOException {
            int  term         = buf.readInt();
            Host candidateId  = Host.serializer.deserialize(buf);
            int  lastLogIndex = buf.readInt();
            int  lastLogTerm  = buf.readInt();
            return new RequestVoteMessage(term, candidateId, lastLogIndex, lastLogTerm);
        }
    };
}
