package protocols.agreement.raft;

import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.raft.messages.*;
import protocols.agreement.raft.timers.ElectionTimer;
import protocols.agreement.raft.timers.HeartbeatTimer;
import protocols.agreement.raft.util.LogEntry;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.RemoveReplicaRequest;

import java.io.IOException;
import java.util.*;

public class RaftAgreement extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(RaftAgreement.class);

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "RaftAgreement";

    private enum Role { FOLLOWER, CANDIDATE, LEADER }
    private Role currentRole;

    // Timeouts passed by props
    private final int electionTimeoutMin;
    private final int electionTimeoutMax;
    private final int heartbeatInterval;

    // Raft Persistent State
    private int currentTerm;
    private Host votedFor;
    private Map<Integer, LogEntry> log;
    private int lastLogIndex;

    // Raft Volatile State (All nodes)
    private int commitIndex;
    private int lastApplied;

    // Raft Volatile State (Leaders only)
    private Map<Host, Integer> nextIndex;
    private Map<Host, Integer> matchIndex;
    private Map<Host, Boolean> isSendingToPeer = new HashMap<>();

    // Node and Quorum Control
    private Host myself;
    private Host currentLeader;
    private int joinedInstance;
    private List<Host> membership;
    private Set<Host> votesReceived;

    // Babel Timer Identifiers
    private long electionTimerId = -1;
    private long heartbeatTimerId = -1;

    public RaftAgreement(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.electionTimeoutMin = Integer.parseInt(props.getProperty("raft.election.timeout.min", "300"));
        this.electionTimeoutMax = Integer.parseInt(props.getProperty("raft.election.timeout.max", "600"));
        this.heartbeatInterval = Integer.parseInt(props.getProperty("raft.heartbeat.interval", "50"));

        logger.info("Raft configured with Election Timeout [{}ms, {}ms] and Heartbeat [{}ms]",
                electionTimeoutMin, electionTimeoutMax, heartbeatInterval);

        this.currentRole = Role.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new HashMap<>();
        this.lastLogIndex = -1; // -1 indicates empty log
        this.commitIndex = -1;
        this.lastApplied = -1;

        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.votesReceived = new HashSet<>();

        this.joinedInstance = -1;
        this.membership = new ArrayList<>();

        /*--------------------- Register SMR Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ElectionTimer.TIMER_ID, this::uponElectionTimeout);
        registerTimerHandler(HeartbeatTimer.TIMER_ID, this::uponHeartbeatTimeout);
    }

    @Override
    public void init(Properties props) {
        // Secondary initialization if necessary
    }

    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created. Network Identity: {}", cId, myself);

        registerSharedChannel(cId);

        /*---------------------- Register Raft Message Serializers ---------------------- */
        registerMessageSerializer(cId, RequestVoteMsg.MSG_ID, RequestVoteMsg.serializer);
        registerMessageSerializer(cId, RequestVoteReplyMsg.MSG_ID, RequestVoteReplyMsg.serializer);
        registerMessageSerializer(cId, AppendEntriesMsg.MSG_ID, AppendEntriesMsg.serializer);
        registerMessageSerializer(cId, AppendEntriesReplyMsg.MSG_ID, AppendEntriesReplyMsg.serializer);
        registerMessageSerializer(cId, ForwardProposalMsg.MSG_ID, ForwardProposalMsg.serializer);

        /*---------------------- Register Network Message Handlers ------------------------- */
        try {
            registerMessageHandler(cId, RequestVoteMsg.MSG_ID, this::uponRequestVote, this::uponMsgFail);
            registerMessageHandler(cId, RequestVoteReplyMsg.MSG_ID, this::uponRequestVoteReply, this::uponMsgFail);
            registerMessageHandler(cId, AppendEntriesMsg.MSG_ID, this::uponAppendEntries, this::uponMsgFail);
            registerMessageHandler(cId, AppendEntriesReplyMsg.MSG_ID, this::uponAppendEntriesReply, this::uponMsgFail);
            registerMessageHandler(cId, ForwardProposalMsg.MSG_ID, this::uponForwardProposalMsg, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Catastrophic error registering Raft network handlers", e);
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        joinedInstance = notification.getJoinInstance();
        membership = new ArrayList<>(notification.getMembership());
        logger.info("Raft active at initial instance {}. Network members: {}", joinedInstance, membership);

        resetElectionTimer();
    }

    /* ------------------------------------------------------------------------- */
    /* Logic and Interface with the SMR Layer                                    */
    /* ------------------------------------------------------------------------- */

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        if (currentRole != Role.LEADER) {
            if (currentLeader != null) {
                logger.info("I am not the leader. Forwarding proposal to leader: {}", currentLeader);
                ForwardProposalMsg msg = new ForwardProposalMsg(
                        request.getInstance(), request.getOpId(), request.getOperation()
                );
                sendMessage(msg, currentLeader);
            } else {
                logger.warn("Rejected ProposeRequest: No known leader.");
            }
            return;
        }

        // DITADURA DO LÍDER: O líder dita o próximo índice sequencial correto
        lastLogIndex++;
        int inst = lastLogIndex;

        LogEntry entry = new LogEntry(currentTerm, inst, request.getOpId(), request.getOperation());
        log.put(inst, entry);

        logger.debug("Leader logged local entry: Instance {}, Term {}", inst, currentTerm);
        /*
        for (Host peer : membership) {
            if (!peer.equals(myself)) {
                sendAppendEntriesToPeer(peer);
            }
        }
        */
    }

    private void uponForwardProposalMsg(ForwardProposalMsg msg, Host from, short sourceProto, int channelId) {
        if (currentRole != Role.LEADER) {
            logger.warn("Received a forwarded proposal from {}, but I am no longer leader! Discarding...", from);
            return;
        }

        // DITADURA DO LÍDER: Mantém a consistência também para mensagens reencaminhadas
        lastLogIndex++;
        int inst = lastLogIndex;

        LogEntry entry = new LogEntry(currentTerm, inst, msg.getOpId(), msg.getOperation());
        log.put(inst, entry);

        logger.info("Leader logged forwarded entry from {}: Instance {}, Term {}", from, inst, currentTerm);
        for (Host peer : membership) {
            if (!peer.equals(myself)) {
                sendAppendEntriesToPeer(peer);
            }
        }
    }

    private void updateLeader(Host newLeader) {
        if (this.currentLeader == null || !this.currentLeader.equals(newLeader)) {
            this.currentLeader = newLeader;
            logger.info("Leadership change detected! New leader: {}", newLeader);
            triggerNotification(new LeaderChangeNotification(newLeader));
        }
    }

    private void checkAndUpdateCommitIndex() {
        // Find the highest N such that N > commitIndex and the majority has replicated up to N
        for (int N = lastLogIndex; N > commitIndex; N--) {
            if (log.containsKey(N) && log.get(N).getTerm() == currentTerm) {
                int replicasWithEntry = 1; // Count the leader itself
                for (Host peer : membership) {
                    if (!peer.equals(myself) && matchIndex.getOrDefault(peer, -1) >= N) {
                        replicasWithEntry++;
                    }
                }
                if (replicasWithEntry > membership.size() / 2) {
                    commitIndex = N;
                    applyLogEntries();
                    break;
                }
            }
        }
    }

    private void applyLogEntries() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            if (entry != null) {
                logger.info("Raft sending stable decision to SMR: Instance {}", lastApplied);
                triggerNotification(new DecidedNotification(lastApplied, entry.getOpId(), entry.getOperation()));
            }
        }
    }

    /* ------------------------------------------------------------------------- */
    /* Raft States and Transitions Management                                    */
    /* ------------------------------------------------------------------------- */

    private void startElection() {
        currentTerm++;
        currentRole = Role.CANDIDATE;
        votedFor = myself;
        votesReceived.clear();
        votesReceived.add(myself);
        resetElectionTimer();

        logger.info("Leader Election started. Term: {}. Looking for votes...", currentTerm);

        int myLastLogTerm = (lastLogIndex >= 0 && log.containsKey(lastLogIndex)) ? log.get(lastLogIndex).getTerm() : 0;

        RequestVoteMsg msg = new RequestVoteMsg(currentTerm, myself, lastLogIndex, myLastLogTerm);
        for (Host peer : membership) {
            if (!peer.equals(myself)) sendMessage(msg, peer);
        }
    }

    private void becomeLeader() {
        currentRole = Role.LEADER;
        stopHeartbeatTimer();
        updateLeader(myself);
        logger.info("Acclamation: I am the new LEADER of the cluster in term {}", currentTerm);

        nextIndex.clear();
        matchIndex.clear();
        for (Host peer : membership) {
            if (!peer.equals(myself)) {
                nextIndex.put(peer, lastLogIndex + 1);
                matchIndex.put(peer, -1);
            }
        }

        startHeartbeatTimer();
    }

    private void    sendAppendEntriesToPeer(Host peer) {
        if (isSendingToPeer.getOrDefault(peer, false)) {
            return;
        }

        int nIndex = nextIndex.getOrDefault(peer, lastLogIndex + 1);
        int prevLogIndex = nIndex - 1;
        int prevLogTerm = (prevLogIndex >= 0 && log.containsKey(prevLogIndex)) ? log.get(prevLogIndex).getTerm() : 0;

        List<LogEntry> entriesToSend = new ArrayList<>();
        int MAX_BATCH_SIZE = 100;
        
        for (int i = nIndex; i <= lastLogIndex && entriesToSend.size() < MAX_BATCH_SIZE; i++) {
            if (log.containsKey(i)) entriesToSend.add(log.get(i));
        }

        isSendingToPeer.put(peer, true);

        AppendEntriesMsg msg = new AppendEntriesMsg(currentTerm, myself, prevLogIndex, prevLogTerm, commitIndex, entriesToSend);
        sendMessage(msg, peer);
    }

    /* ------------------------------------------------------------------------- */
    /* Network Communication (RPC) Handlers                                      */
    /* ------------------------------------------------------------------------- */

    private void uponRequestVote(RequestVoteMsg msg, Host host, short sourceProto, int channelId) {
        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            currentRole = Role.FOLLOWER;
            votedFor = null;
            stopHeartbeatTimer();
            resetElectionTimer();
        }

        boolean voteGranted = false;
        if (msg.getTerm() == currentTerm && (votedFor == null || votedFor.equals(msg.getCandidate()))) {
            int myLastLogTerm = (lastLogIndex >= 0 && log.containsKey(lastLogIndex)) ? log.get(lastLogIndex).getTerm() : 0;

            // Safety Rule: Is the candidate's log at least as up-to-date as mine?
            boolean logUpToDate = false;
            if (msg.getLastLogTerm() > myLastLogTerm) {
                logUpToDate = true;
            } else if (msg.getLastLogTerm() == myLastLogTerm && msg.getLastLogIndex() >= lastLogIndex) {
                logUpToDate = true;
            }

            if (logUpToDate) {
                voteGranted = true;
                votedFor = msg.getCandidate();
                resetElectionTimer();
                logger.info("Vote granted to replica {} for term {}", msg.getCandidate(), currentTerm);
            }
        }

        sendMessage(new RequestVoteReplyMsg(currentTerm, voteGranted), host);
    }

    private void uponRequestVoteReply(RequestVoteReplyMsg msg, Host host, short sourceProto, int channelId) {
        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            currentRole = Role.FOLLOWER;
            votedFor = null;
            stopHeartbeatTimer();
            resetElectionTimer();
            return;
        }

        if (currentRole == Role.CANDIDATE && msg.getTerm() == currentTerm && msg.isVoteGranted()) {
            votesReceived.add(host);
            if (votesReceived.size() > membership.size() / 2) {
                becomeLeader();
            }
        }
    }

    private void uponAppendEntries(AppendEntriesMsg msg, Host host, short sourceProto, int channelId) {
        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            currentRole = Role.FOLLOWER;
            votedFor = null;
            stopHeartbeatTimer();
            resetElectionTimer();
        }

        boolean success = false;
        int lastLogIndexState = msg.getPrevLogIndex();

        if (msg.getTerm() == currentTerm) {
            if (currentRole == Role.CANDIDATE) currentRole = Role.FOLLOWER;
            resetElectionTimer();
            updateLeader(msg.getLeader());

            // Validate Log Matching Invariant
            if (msg.getPrevLogIndex() == -1 || (log.containsKey(msg.getPrevLogIndex()) && log.get(msg.getPrevLogIndex()).getTerm() == msg.getPrevLogTerm())) {
                success = true;
                int idx = msg.getPrevLogIndex();

                for (LogEntry entry : msg.getEntries()) {
                    idx = entry.getIndex();
                    // Detect and resolve inconsistencies by cleaning orphan entries
                    if (log.containsKey(idx) && log.get(idx).getTerm() != entry.getTerm()) {
                        List<Integer> keysToRemove = new ArrayList<>();
                        for (int k : log.keySet()) {
                            if (k >= idx) keysToRemove.add(k);
                        }
                        for (int k : keysToRemove) log.remove(k);
                    }
                    if (!log.containsKey(idx)) {
                        log.put(idx, entry);
                    }
                }

                lastLogIndexState = Math.max(msg.getPrevLogIndex(), idx);
                if (lastLogIndexState > lastLogIndex) {
                    lastLogIndex = lastLogIndexState;
                }

                // Commit entries consolidated by the Leader
                if (msg.getLeaderCommit() > commitIndex) {
                    commitIndex = Math.min(msg.getLeaderCommit(), lastLogIndex);
                    applyLogEntries();
                }
            }
        }

        sendMessage(new AppendEntriesReplyMsg(currentTerm, success, lastLogIndexState, myself), host);
    }

    private void uponAppendEntriesReply(AppendEntriesReplyMsg msg, Host host, short sourceProto, int channelId) {
        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            currentRole = Role.FOLLOWER;
            votedFor = null;
            stopHeartbeatTimer();
            resetElectionTimer();
            return;
        }

        if (currentRole == Role.LEADER && msg.getTerm() == currentTerm) {
            isSendingToPeer.put(host, false);

            if (msg.isSuccess()) {
                if (msg.getMatchIndex() > matchIndex.getOrDefault(host, -1)) {
                    matchIndex.put(host, msg.getMatchIndex());
                    nextIndex.put(host, msg.getMatchIndex() + 1);
                    checkAndUpdateCommitIndex();
                }

                if (lastLogIndex >= nextIndex.get(host)) {
                    sendAppendEntriesToPeer(host);
                }
            } else {
                // Synchronization failed: decrement replication pointer and retry
                int currentNext = nextIndex.getOrDefault(host, 0);
                nextIndex.put(host, Math.max(0, currentNext - 1));
                sendAppendEntriesToPeer(host);
            }
        }
    }

    /* ------------------------------------------------------------------------- */
    /* Timing (Timers) Handlers                                                  */
    /* ------------------------------------------------------------------------- */

    private void uponElectionTimeout(ElectionTimer timer, long timerId) {
        if (currentRole != Role.LEADER) {
            logger.warn("Election Timeout reached! Initiating procedures...");
            startElection();
        }
    }

    private void uponHeartbeatTimeout(HeartbeatTimer timer, long timerId) {
        if (currentRole == Role.LEADER) {
            for (Host peer : membership) {
                if (!peer.equals(myself)) {
                    if (isSendingToPeer.getOrDefault(peer, false)) {
                        // if channel is occupied, we send an empty heartbeat.
                        sendEmptyHeartbeat(peer);
                    } else {
                        // if not, we also try to send pending data!
                        sendAppendEntriesToPeer(peer);
                    }
                }
            }
        }
    }

    private void sendEmptyHeartbeat(Host peer) {
        int nIndex = nextIndex.getOrDefault(peer, lastLogIndex + 1);
        int prevLogIndex = nIndex - 1;
        int prevLogTerm = (prevLogIndex >= 0 && log.containsKey(prevLogIndex)) ? log.get(prevLogIndex).getTerm() : 0;

        AppendEntriesMsg msg = new AppendEntriesMsg(currentTerm, myself, prevLogIndex, prevLogTerm, commitIndex, new ArrayList<>());
        sendMessage(msg , peer);
    }

    private void resetElectionTimer() {
        if (electionTimerId != -1) cancelTimer(electionTimerId);

        // Calculate random timeout using limits passed by properties
        int range = electionTimeoutMax - electionTimeoutMin;
        long timeout = electionTimeoutMin + (range > 0 ? new Random().nextInt(range) : 0);

        electionTimerId = setupTimer(new ElectionTimer(), timeout);
    }

    private void startHeartbeatTimer() {
        if (heartbeatTimerId == -1) {
            heartbeatTimerId = setupPeriodicTimer(new HeartbeatTimer(), heartbeatInterval, heartbeatInterval);
        }
    }

    private void stopHeartbeatTimer() {
        if (heartbeatTimerId != -1) {
            cancelTimer(heartbeatTimerId);
            heartbeatTimerId = -1;
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Physical network failure when sending {} to {}. Cause: {}", msg, host, throwable.getMessage());
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        if (!membership.contains(request.getReplica())) membership.add(request.getReplica());
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        membership.remove(request.getReplica());
    }
}