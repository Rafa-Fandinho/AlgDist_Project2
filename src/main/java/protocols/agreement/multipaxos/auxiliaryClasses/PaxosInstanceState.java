package protocols.agreement.multipaxos.auxiliaryClasses;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PaxosInstanceState {
    private UUID acceptedOpId;
    private byte[] acceptedOperation;
    private Ballot acceptedBallot;
    private boolean decided;private Set<byte[]> proposedOps;
    private Set<byte[]> decidedOps;
    private Set<Host> acceptOkResponses;
    private boolean isMembershipOp;

    public PaxosInstanceState(UUID acceptedOpId, byte[] acceptedOperation, Ballot acceptedBallot, boolean decided, Set<Host> acceptOkResponses, boolean isMembershipOp){
        this.acceptedOpId = acceptedOpId;
        this.acceptedOperation = acceptedOperation;
        this.acceptedBallot = acceptedBallot;
        this.decided = decided;
        this.acceptOkResponses = acceptOkResponses;
        this.isMembershipOp = isMembershipOp;
    }

    public PaxosInstanceState(UUID acceptedOpId, byte[] acceptedOperation, Ballot acceptedBallot){
        this.acceptedOpId = acceptedOpId;
        this.acceptedOperation = acceptedOperation;
        this.acceptedBallot = acceptedBallot;
        this.decided = false;
        this.acceptOkResponses = new HashSet<>();
        this.isMembershipOp = false;
    }

    public PaxosInstanceState(UUID acceptedOpId, byte[] acceptedOperation, Ballot acceptedBallot, boolean isMembershipOp){
        this.acceptedOpId = acceptedOpId;
        this.acceptedOperation = acceptedOperation;
        this.acceptedBallot = acceptedBallot;
        this.decided = false;
        this.acceptOkResponses = new HashSet<>();
        this.isMembershipOp = isMembershipOp;
    }

    public UUID getAcceptedOpId() {
        return acceptedOpId;
    }

    public byte[] getAcceptedOperation() {
        return acceptedOperation;
    }

    public Ballot getAcceptedBallot() {
        return acceptedBallot;
    }

    public boolean isDecided() {
        return decided;
    }

    public boolean isMembershipOp() { return isMembershipOp; }

    public Set<Host> getAcceptOkResponses() {
        return acceptOkResponses;
    }

    public void addAcceptOkResponse(Host host){
        acceptOkResponses.add(host);
    }

    public void setAcceptOkResponses(Set<Host> acceptOkResponses) {
        this.acceptOkResponses = acceptOkResponses;
    }

    public void setAcceptedBallot(Ballot acceptedBallot) {
        this.acceptedBallot = acceptedBallot;
    }

    public void setAcceptedOperation(byte[] acceptedOperation) {
        this.acceptedOperation = acceptedOperation;
    }

    public void setAcceptedOpId(UUID acceptedOpId) {
        this.acceptedOpId = acceptedOpId;
    }

    public void setDecided(boolean decided) {
        this.decided = decided;
    }

    public void setMembershipOp(boolean membershipOp) { isMembershipOp = membershipOp; }
}
