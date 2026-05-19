package protocols.agreement.raft;

import protocols.agreement.raft.messages.AppendEntriesMessage;
import protocols.agreement.raft.messages.AppendEntriesReply;
import protocols.agreement.raft.messages.RequestVoteMessage;
import protocols.agreement.raft.messages.RequestVoteReply;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Raft Agreement Protocol
 *
 * Implements the Raft consensus algorithm as described in:
 * "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout, 2014)
 *
 * The implementation follows Figure 2 of the paper.
 *
 * Key sub-problems addressed:
 *  1. Leader election   – randomized election timeouts + RequestVote RPCs
 *  2. Log replication   – AppendEntries RPCs (also serve as heartbeats)
 *  3. Safety            – term numbers, log-matching invariant, commit rules
 */
public class RaftAgreement extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(RaftAgreement.class);

    // ── Protocol identity ────────────────────────────────────────────────────
    public static final short PROTOCOL_ID   = 102;
    public static final String PROTOCOL_NAME = "RaftAgreement";

    // ── Timer IDs ────────────────────────────────────────────────────────────
    private static final short ELECTION_TIMEOUT_TIMER = 1;
    private static final short HEARTBEAT_TIMER        = 2;

    // ── Default configuration values ─────────────────────────────────────────
    private static final int DEFAULT_ELECTION_TIMEOUT_MIN_MS = 2000;
    private static final int DEFAULT_ELECTION_TIMEOUT_MAX_MS = 4000;
    private static final int DEFAULT_HEARTBEAT_INTERVAL_MS   = 1000;

    // ── Configuration (loaded from props) ────────────────────────────────────
    private final int electionTimeoutMin;
    private final int electionTimeoutMax;
    private final int heartbeatInterval;
    private final Random rand = new Random();

    // ── Babel / network ──────────────────────────────────────────────────────
    private Host myself;
    private int channelId;

    // ── Membership ───────────────────────────────────────────────────────────
    private List<Host> membership;  // current cluster membership
    private int joinedInstance;     // instance at which we joined (-1 = not yet joined)

    // ── Server role ──────────────────────────────────────────────────────────
    private enum Role { FOLLOWER, CANDIDATE, LEADER }
    private Role role;

    // ── Persistent state (Figure 2) ──────────────────────────────────────────
    /** Latest term this server has seen */
    private int currentTerm;
    /** candidateId that received vote in currentTerm (null = none) */
    private Host votedFor;
    /**
     * The replicated log.
     * index 0 is a sentinel (term 0, null op) so real entries start at index 1.
     */
    private final List<LogEntry> log;

    // ── Volatile state (all servers) ─────────────────────────────────────────
    /** Index of highest log entry known to be committed */
    private int commitIndex;
    /** Index of highest log entry applied to state machine */
    private int lastApplied;

    // ── Volatile state (leader only, re-initialized on election) ─────────────
    /** For each follower, index of the next log entry to send */
    private Map<Host, Integer> nextIndex;
    /** For each follower, index of highest log entry known to be replicated */
    private Map<Host, Integer> matchIndex;

    // ── Election tracking ────────────────────────────────────────────────────
    private int votesReceived;

    // ── Pending proposals (buffered while we are not leader yet) ─────────────
    /** Operations proposed by the SMR layer that have not yet been appended to the log */
    private final Queue<ProposeRequest> pendingProposals = new LinkedList<>();

    // =========================================================================
    // Inner helper class: a log entry
    // =========================================================================
    private static class LogEntry {
        final int  term;
        final UUID opId;
        final byte[] op;

        LogEntry(int term, UUID opId, byte[] op) {
            this.term = term;
            this.opId = opId;
            this.op   = op;
        }
    }

    private static class RaftTimer extends ProtoTimer {
        RaftTimer(short id) {
            super(id);
        }

        @Override
        public ProtoTimer clone() {
            return new RaftTimer(this.getId());
        }
    }

    // =========================================================================
    // Constructor
    // =========================================================================
    public RaftAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        // Load configurable parameters
        electionTimeoutMin = Integer.parseInt(
                props.getProperty("raft.electionTimeoutMin",
                        String.valueOf(DEFAULT_ELECTION_TIMEOUT_MIN_MS)));
        electionTimeoutMax = Integer.parseInt(
                props.getProperty("raft.electionTimeoutMax",
                        String.valueOf(DEFAULT_ELECTION_TIMEOUT_MAX_MS)));
        heartbeatInterval  = Integer.parseInt(
                props.getProperty("raft.heartbeatInterval",
                        String.valueOf(DEFAULT_HEARTBEAT_INTERVAL_MS)));

        // Initialise persistent state
        currentTerm = 0;
        votedFor    = null;
        log         = new ArrayList<>();
        log.add(new LogEntry(0, null, null)); // sentinel at index 0

        // Initialise volatile state
        commitIndex  = 0;
        lastApplied  = 0;

        // Not yet part of the system
        joinedInstance = -1;
        membership     = null;
        role           = Role.FOLLOWER;

        // ── Register timer handlers ──
        registerTimerHandler(ELECTION_TIMEOUT_TIMER, this::uponElectionTimeout);
        registerTimerHandler(HEARTBEAT_TIMER,        this::uponHeartbeatTimer);

        // ── Register request handlers ──
        registerRequestHandler(ProposeRequest.REQUEST_ID,      this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID,   this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        // ── Register notification handlers ──
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID,       this::uponJoinedNotification);
    }

    // =========================================================================
    // init
    // =========================================================================
    @Override
    public void init(Properties props) {
        // Nothing here; we wait for ChannelReadyNotification and then JoinedNotification.
    }

    // =========================================================================
    // Channel ready – register serializers and message handlers
    // =========================================================================
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        channelId = notification.getChannelId();
        myself    = notification.getMyself();
        logger.info("Channel {} created, I am {}", channelId, myself);

        registerSharedChannel(channelId);

        // Serializers
        registerMessageSerializer(channelId, RequestVoteMessage.MSG_ID,    RequestVoteMessage.serializer);
        registerMessageSerializer(channelId, RequestVoteReply.MSG_ID,      RequestVoteReply.serializer);
        registerMessageSerializer(channelId, AppendEntriesMessage.MSG_ID,  AppendEntriesMessage.serializer);
        registerMessageSerializer(channelId, AppendEntriesReply.MSG_ID,    AppendEntriesReply.serializer);

        // Message handlers
        try {
            registerMessageHandler(channelId, RequestVoteMessage.MSG_ID,
                    this::uponRequestVote,    this::uponMsgFail);
            registerMessageHandler(channelId, RequestVoteReply.MSG_ID,
                    this::uponRequestVoteReply, this::uponMsgFail);
            registerMessageHandler(channelId, AppendEntriesMessage.MSG_ID,
                    this::uponAppendEntries,  this::uponMsgFail);
            registerMessageHandler(channelId, AppendEntriesReply.MSG_ID,
                    this::uponAppendEntriesReply, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering Raft message handlers", e);
        }
    }

    // =========================================================================
    // Joined notification – we are now part of the cluster
    // =========================================================================
    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        joinedInstance = notification.getJoinInstance();
        membership     = new ArrayList<>(notification.getMembership());
        logger.info("Raft starting at instance {}, membership: {}", joinedInstance, membership);

        // Start as a follower and wait for a heartbeat; if none arrives, start an election.
        becomeFollower(currentTerm);
        resetElectionTimer();
    }

    // =========================================================================
    // ── Role transitions ──────────────────────────────────────────────────────
    // =========================================================================

    private void becomeFollower(int term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor    = null;
        }
        Role oldRole = role;
        role = Role.FOLLOWER;
        if (oldRole == Role.LEADER) {
            logger.info("Stepped down from LEADER in term {}", currentTerm);
            cancelTimer(HEARTBEAT_TIMER);
        }
    }

    private void becomeCandidate() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor      = myself;  // vote for self
        votesReceived = 1;
        logger.info("Starting election for term {}", currentTerm);

        // Send RequestVote to all other members
        int lastLogIndex = lastLogIndex();
        int lastLogTerm  = lastLogTerm();
        RequestVoteMessage rv = new RequestVoteMessage(currentTerm, myself, lastLogIndex, lastLogTerm);
        membership.forEach(h -> { if (!h.equals(myself)) sendMessage(rv, h); });

        resetElectionTimer();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        logger.info("I am the new LEADER for term {}", currentTerm);

        // Initialize leader state
        nextIndex  = new HashMap<>();
        matchIndex = new HashMap<>();
        int nextIdx = lastLogIndex() + 1;
        for (Host h : membership) {
            if (!h.equals(myself)) {
                nextIndex.put(h, nextIdx);
                matchIndex.put(h, 0);
            }
        }

        // Notify the SMR layer of the leadership change
        triggerNotification(new LeaderChangeNotification(myself));

        // Cancel election timer; start sending heartbeats
        cancelTimer(ELECTION_TIMEOUT_TIMER);
        setupPeriodicTimer(new RaftTimer(HEARTBEAT_TIMER), 0, heartbeatInterval);

        // Drain any pending proposals that arrived while we were not the leader
        while (!pendingProposals.isEmpty()) {
            processProposeAsLeader(pendingProposals.poll());
        }
    }

    // =========================================================================
    // ── Timers ────────────────────────────────────────────────────────────────
    // =========================================================================

    private void resetElectionTimer() {
        cancelTimer(ELECTION_TIMEOUT_TIMER);
        int timeout = electionTimeoutMin
                + rand.nextInt(electionTimeoutMax - electionTimeoutMin + 1);
        setupTimer(new RaftTimer(ELECTION_TIMEOUT_TIMER), timeout);
    }

    private void uponElectionTimeout(ProtoTimer timer, long timerId) {
        if (joinedInstance < 0) return; // not yet joined
        if (role == Role.LEADER)  return; // leaders do not time out

        logger.debug("Election timeout in term {}, starting new election", currentTerm);
        becomeCandidate();
    }

    private void uponHeartbeatTimer(ProtoTimer timer, long timerId) {
        if (role != Role.LEADER) return;
        // Send AppendEntries (possibly empty) to all followers
        membership.forEach(h -> { if (!h.equals(myself)) sendAppendEntries(h); });
    }

    // =========================================================================
    // ── RequestVote RPC ───────────────────────────────────────────────────────
    // =========================================================================

    /** Receiver – section 5.2 & 5.4 of the paper */
    private void uponRequestVote(RequestVoteMessage msg, Host sender, short sourceProto, int channelId) {
        logger.debug("Received RequestVote from {} for term {}", sender, msg.getTerm());

        if (msg.getTerm() > currentTerm) {
            becomeFollower(msg.getTerm());
            resetElectionTimer();
        }

        boolean granted = false;
        if (msg.getTerm() >= currentTerm) {
            // Grant vote if we haven't voted yet (or already voted for this candidate)
            // AND the candidate's log is at least as up-to-date as ours (§5.4.1)
            boolean notVoted = (votedFor == null || votedFor.equals(sender));
            boolean candidateLogUpToDate = isCandidateLogUpToDate(
                    msg.getLastLogIndex(), msg.getLastLogTerm());

            if (notVoted && candidateLogUpToDate) {
                votedFor = sender;
                granted  = true;
                // Reset election timer since we just voted – we don't want to disrupt a live leader
                resetElectionTimer();
            }
        }

        logger.debug("Vote for {} in term {}: {}", sender, msg.getTerm(), granted);
        sendMessage(new RequestVoteReply(currentTerm, granted), sender);
    }

    /** Returns true if the candidate's log is at least as up-to-date as ours (§5.4.1) */
    private boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastTerm  = lastLogTerm();
        int myLastIndex = lastLogIndex();

        if (candidateLastLogTerm != myLastTerm) {
            return candidateLastLogTerm > myLastTerm;
        }
        return candidateLastLogIndex >= myLastIndex;
    }

    /** Sender – handle vote reply */
    private void uponRequestVoteReply(RequestVoteReply msg, Host sender, short sourceProto, int channelId) {
        logger.debug("Received vote reply from {} granted={} term={}", sender, msg.isGranted(), msg.getTerm());

        if (msg.getTerm() > currentTerm) {
            becomeFollower(msg.getTerm());
            resetElectionTimer();
            return;
        }

        if (role != Role.CANDIDATE || msg.getTerm() != currentTerm) return;

        if (msg.isGranted()) {
            votesReceived++;
            logger.debug("Got vote, total votes: {}/{}", votesReceived, membership.size());
            if (votesReceived >= majority()) {
                becomeLeader();
            }
        }
    }

    // =========================================================================
    // ── AppendEntries RPC ────────────────────────────────────────────────────
    // =========================================================================

    /** Receiver – Figure 2 of the paper */
    private void uponAppendEntries(AppendEntriesMessage msg, Host sender, short sourceProto, int channelId) {
        logger.debug("AppendEntries from {} term={} prevLogIndex={} entries={}",
                sender, msg.getTerm(), msg.getPrevLogIndex(), msg.getEntries().size());

        // Reply false if term < currentTerm (§5.1)
        if (msg.getTerm() < currentTerm) {
            sendMessage(new AppendEntriesReply(currentTerm, false, 0), sender);
            return;
        }

        // Valid leader contact – step down if needed and reset timer
        if (msg.getTerm() > currentTerm || role == Role.CANDIDATE) {
            becomeFollower(msg.getTerm());
        }
        // Even if already a follower with the same term we still reset the timer
        resetElectionTimer();

        // Reply false if log doesn't contain an entry at prevLogIndex with prevLogTerm (§5.3)
        int prevLogIndex = msg.getPrevLogIndex();
        int prevLogTerm  = msg.getPrevLogTerm();
        if (prevLogIndex > 0) {
            if (prevLogIndex >= log.size()) {
                // We don't have that index at all
                sendMessage(new AppendEntriesReply(currentTerm, false, log.size() - 1), sender);
                return;
            }
            if (log.get(prevLogIndex).term != prevLogTerm) {
                // Term mismatch – delete conflicting entry and all that follow (§5.3)
                log.subList(prevLogIndex, log.size()).clear();
                sendMessage(new AppendEntriesReply(currentTerm, false, prevLogIndex - 1), sender);
                return;
            }
        }

        // Append any new entries (§5.3)
        List<AppendEntriesMessage.Entry> entries = msg.getEntries();
        int insertIdx = prevLogIndex + 1;
        for (AppendEntriesMessage.Entry entry : entries) {
            if (insertIdx < log.size()) {
                // If existing entry conflicts with new entry, delete it and all following
                if (log.get(insertIdx).term != entry.term) {
                    log.subList(insertIdx, log.size()).clear();
                    log.add(new LogEntry(entry.term, entry.opId, entry.op));
                }
                // Otherwise the entry already matches – skip
            } else {
                log.add(new LogEntry(entry.term, entry.opId, entry.op));
            }
            insertIdx++;
        }

        // Update commitIndex (§5.3)
        if (msg.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(msg.getLeaderCommit(), lastLogIndex());
            applyCommitted();
        }

        sendMessage(new AppendEntriesReply(currentTerm, true, lastLogIndex()), sender);
    }

    /** Sender – handle AppendEntries reply */
    private void uponAppendEntriesReply(AppendEntriesReply msg, Host sender, short sourceProto, int channelId) {
        if (role != Role.LEADER) return;

        if (msg.getTerm() > currentTerm) {
            becomeFollower(msg.getTerm());
            resetElectionTimer();
            return;
        }

        if (msg.getTerm() != currentTerm) return;

        if (msg.isSuccess()) {
            // Update matchIndex and nextIndex for this follower
            int matchIdx = msg.getMatchIndex();
            if (matchIdx > matchIndex.getOrDefault(sender, 0)) {
                matchIndex.put(sender, matchIdx);
                nextIndex.put(sender,  matchIdx + 1);
            }
            // Check whether we can advance commitIndex (§5.3, §5.4)
            advanceCommitIndex();
        } else {
            // Decrement nextIndex and retry
            int ni = nextIndex.getOrDefault(sender, 1);
            nextIndex.put(sender, Math.max(1, msg.getMatchIndex() + 1));
            sendAppendEntries(sender);
        }
    }

    /**
     * Leader: check if a higher N exists such that
     *   N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm.
     * If so, set commitIndex = N  (§5.3).
     */
    private void advanceCommitIndex() {
        int n = commitIndex + 1;
        int lastIdx = lastLogIndex();
        while (n <= lastIdx) {
            if (log.get(n).term == currentTerm) {
                int replicatedOn = 1; // leader itself
                for (Host h : membership) {
                    if (!h.equals(myself) && matchIndex.getOrDefault(h, 0) >= n) {
                        replicatedOn++;
                    }
                }
                if (replicatedOn >= majority()) {
                    commitIndex = n;
                    logger.debug("Advanced commitIndex to {}", commitIndex);
                } else {
                    break; // can't commit n yet; no point checking higher
                }
            }
            n++;
        }
        applyCommitted();
    }

    /** Apply all committed but not-yet-applied entries to the state machine */
    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            logger.debug("Delivering decided instance {} opId {}", lastApplied, entry.opId);
            triggerNotification(new DecidedNotification(lastApplied, entry.opId, entry.op));
        }
    }

    // =========================================================================
    // ── Send AppendEntries to one follower ────────────────────────────────────
    // =========================================================================
    private void sendAppendEntries(Host follower) {
        int ni           = nextIndex.getOrDefault(follower, 1);
        int prevLogIndex = ni - 1;
        int prevLogTerm  = (prevLogIndex > 0 && prevLogIndex < log.size())
                ? log.get(prevLogIndex).term : 0;

        List<AppendEntriesMessage.Entry> entries = new ArrayList<>();
        for (int i = ni; i < log.size(); i++) {
            LogEntry le = log.get(i);
            entries.add(new AppendEntriesMessage.Entry(le.term, le.opId, le.op));
        }

        AppendEntriesMessage msg = new AppendEntriesMessage(
                currentTerm, myself, prevLogIndex, prevLogTerm, entries, commitIndex);
        sendMessage(msg, follower);
    }

    // =========================================================================
    // ── Propose request from the SMR layer ───────────────────────────────────
    // =========================================================================
    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("ProposeRequest instance={}", request.getInstance());
        if (joinedInstance < 0) return; // not yet joined

        if (role == Role.LEADER) {
            processProposeAsLeader(request);
        } else {
            // Buffer it; will be drained when/if we become leader
            // (the SMR layer is also expected to forward proposals to the actual leader,
            //  so this buffer is a safety net for the transitional period)
            pendingProposals.offer(request);
        }
    }

    private void processProposeAsLeader(ProposeRequest request) {
        // Append to our own log
        LogEntry entry = new LogEntry(currentTerm, request.getOpId(), request.getOperation());
        log.add(entry);
        int entryIndex = lastLogIndex();
        logger.debug("Leader appended entry index={} term={}", entryIndex, currentTerm);

        // Update own matchIndex
        matchIndex.put(myself, entryIndex);

        // Replicate to followers immediately
        membership.forEach(h -> { if (!h.equals(myself)) sendAppendEntries(h); });
    }

    // =========================================================================
    // ── Membership changes ────────────────────────────────────────────────────
    // =========================================================================

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("AddReplica: {}", request.getReplica());
        Host newReplica = request.getReplica();
        if (!membership.contains(newReplica)) {
            membership.add(newReplica);
            if (role == Role.LEADER) {
                nextIndex.put(newReplica,  lastLogIndex() + 1);
                matchIndex.put(newReplica, 0);
            }
        }
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("RemoveReplica: {}", request.getReplica());
        membership.remove(request.getReplica());
        if (role == Role.LEADER) {
            nextIndex.remove(request.getReplica());
            matchIndex.remove(request.getReplica());
        }
    }

    // =========================================================================
    // ── Helpers ───────────────────────────────────────────────────────────────
    // =========================================================================

    private int lastLogIndex() {
        return log.size() - 1;
    }

    private int lastLogTerm() {
        if (log.size() <= 1) return 0;
        return log.get(log.size() - 1).term;
    }

    private int majority() {
        return membership.size() / 2 + 1;
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                              Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed: {}", msg, host, throwable);
    }
}
