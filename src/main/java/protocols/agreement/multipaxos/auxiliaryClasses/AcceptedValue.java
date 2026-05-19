package protocols.agreement.multipaxos.auxiliaryClasses;

import java.util.UUID;

public class AcceptedValue {
    private final Ballot ballot;
    private final UUID opId;
    private final byte[] op;

    public Ballot getBallot() {
        return ballot;
    }

    public byte[] getOp() {
        return op;
    }

    public UUID getOpId() {
        return opId;
    }

    public AcceptedValue(Ballot ballot, UUID opId, byte[] op) {
        this.ballot = ballot;
        this.opId = opId;
        this.op = op;
    }
}