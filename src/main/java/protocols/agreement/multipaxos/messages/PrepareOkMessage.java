package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multipaxos.auxiliaryClasses.AcceptedValue;
import protocols.agreement.multipaxos.auxiliaryClasses.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PrepareOkMessage extends ProtoMessage {
    public final static short MSG_ID = 103; // Note: Use a unique ID (Prepare was 101, Accept 102)

    private final Ballot promised; //Highest instance already compromised
    private final Map<Integer, AcceptedValue> acceptedInstances;


    public PrepareOkMessage(Ballot highestPromisedBallot, Map<Integer, AcceptedValue> acceptedInstances) {
        super(MSG_ID);
        this.promised = highestPromisedBallot;
        this.acceptedInstances = acceptedInstances != null ? acceptedInstances : new HashMap<>();
    }

    public Ballot getPromised() {
        return promised;
    }

    public Map<Integer, AcceptedValue> getAcceptedInstances() {
        return acceptedInstances;
    }

    @Override
    public String toString() {  //Pedi pro Gemini
        StringBuilder sb = new StringBuilder();
        sb.append("PrepareOkMessage{")
                .append("highestPromisedBallot=").append(promised)
                .append(", acceptedInstances=[");

        acceptedInstances.forEach((instance, accVal) -> sb.append(String.format("(Inst: %d, Ballot: %s, OpId: %s) ",
                instance, accVal.getBallot().toString(), accVal.getOpId())));

        sb.append("]}");
        return sb.toString();
    }

    public static ISerializer<PrepareOkMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PrepareOkMessage msg, ByteBuf out) {
            Ballot.serialize(msg.promised, out);
            out.writeInt(msg.acceptedInstances.size());
            for (Map.Entry<Integer, AcceptedValue> entry : msg.acceptedInstances.entrySet()) {
                out.writeInt(entry.getKey());
                AcceptedValue val = entry.getValue();
                Ballot.serialize(val.getBallot(), out);
                out.writeLong(val.getOpId().getMostSignificantBits());
                out.writeLong(val.getOpId().getLeastSignificantBits());
                out.writeInt(val.getOp().length);
                out.writeBytes(val.getOp());
                out.writeBoolean(val.isMembershipOp());
            }
        }

        @Override
        public PrepareOkMessage deserialize(ByteBuf in) {
            Ballot promised = Ballot.deserialize(in);
            int mapSize = in.readInt();
            Map<Integer, AcceptedValue> acceptedInstances = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                int instance = in.readInt();
                Ballot ballot = Ballot.deserialize(in);
                long highBytes = in.readLong();
                long lowBytes = in.readLong();
                UUID opId = new UUID(highBytes, lowBytes);
                byte[] op = new byte[in.readInt()];
                in.readBytes(op);
                boolean isMembership = in.readBoolean();
                acceptedInstances.put(instance, new AcceptedValue(ballot, opId, op, isMembership));
            }

            return new PrepareOkMessage(promised, acceptedInstances);
        }
    };
}