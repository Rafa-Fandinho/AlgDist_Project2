package protocols.agreement.raft.util;

import java.util.UUID;

public class LogEntry {
    private final int term;
    private final int index;
    private final UUID opId;
    private final byte[] operation;

    public LogEntry(int term, int index, UUID opId, byte[] operation) {
        this.term = term;
        this.index = index;
        this.opId = opId;
        this.operation = operation;
    }
    public int getTerm() { return term; }
    public int getIndex() { return index; }
    public UUID getOpId() { return opId; }
    public byte[] getOperation() { return operation; }
}