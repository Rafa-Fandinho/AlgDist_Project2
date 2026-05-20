package protocols.agreement.multipaxos.auxiliaryClasses;

import java.util.UUID;

public class AcceptedValue {
    private final Ballot ballot;
    private final UUID opId;
    private final byte[] op;
    private final boolean isMembershipOp;

    public Ballot getBallot() {
        return ballot;
    }

    public byte[] getOp() {
        return op;
    }

    public UUID getOpId() {
        return opId;
    }

    public boolean isMembershipOp() { return isMembershipOp; }

    public AcceptedValue(Ballot ballot, UUID opId, byte[] op, boolean isMO) {
        this.ballot = ballot;
        this.opId = opId;
        this.op = op;
        this.isMembershipOp = isMO;
    }
}