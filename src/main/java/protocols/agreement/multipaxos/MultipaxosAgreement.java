package protocols.agreement.multipaxos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.multipaxos.auxiliaryClasses.AcceptedValue;
import protocols.agreement.multipaxos.auxiliaryClasses.Ballot;
import protocols.agreement.multipaxos.auxiliaryClasses.PaxosInstanceState;
import protocols.agreement.multipaxos.messages.*;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.multipaxos.timers.LeaderTimer;
import protocols.agreement.multipaxos.timers.SuspectTimer;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.UnknownHostException;
import java.util.*;

public class MultipaxosAgreement extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(MultipaxosAgreement.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 101;
    public final static String PROTOCOL_NAME = "Multi-Paxos Agreement";

    public static final byte OP_TYPE_ADD = 0x01;
    public static final byte OP_TYPE_REMOVE = 0x02;

    private Host myself;
    private int joinedInstance;
    private List<Host> membership;
    private Map<Integer, PaxosInstanceState> instances;
    private Ballot promisedBallot;
    private Ballot currentBallot;   //probably can be merged with promised ballot
    private int slot_in;
    private int slot_out;
    private boolean isLeader;
    private Host currentLeader;
    private Set<Host> prepareOkResponses;
    private long lastHeartbeatTime;
    private boolean electionInProgress;


    public final static String PAR_ACK_TIME = "agreement.heartbeat_timeout";  //TODO: Check if we need this later or hardcoding timers is ok
    public final static String PAR_DEFAULT_ACK_TIME = "5000"; //1 seconds
    private final int heartbeatTimeout; //param: timeout for acknowledgement messages*/

    public MultipaxosAgreement(Properties props) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        this.heartbeatTimeout = Integer.parseInt(props.getProperty(PAR_ACK_TIME, PAR_DEFAULT_ACK_TIME));

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(LeaderTimer.TIMER_ID, this::uponLeaderTimer);
        registerTimerHandler(SuspectTimer.TIMER_ID, this::uponSuspectTimer);
        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        //TODO: Revise
        this.prepareOkResponses = new HashSet<>();
        this.slot_in = 0;
        this.slot_out = 0;
        this.instances = new HashMap<>();
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.isLeader = false;
        this.electionInProgress = false;
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOkMessage.MSG_ID, AcceptOkMessage.serializer);
        registerMessageSerializer(cId, DecideMessage.MSG_ID, DecideMessage.serializer);
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOkMessage.MSG_ID, PrepareOkMessage.serializer);
        registerMessageSerializer(cId,ForwardProposalMessage.MSG_ID, ForwardProposalMessage.serializer);
        registerMessageSerializer(cId, HeartbeatMessage.MSG_ID, HeartbeatMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, DecideMessage.MSG_ID, this::uponDecideMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOkMessage.MSG_ID, this::uponPrepareOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, ForwardProposalMessage.MSG_ID, this::uponForwardProposalMessage, this::uponMsgFail);
            registerMessageHandler(cId, HeartbeatMessage.MSG_ID, this::uponHeartbeatMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    private void uponLeaderTimer(LeaderTimer timer, long timerId){
        logger.debug("Leader Time: instance {}", slot_out);
        HeartbeatMessage message = new HeartbeatMessage(-1);
        membership.forEach(h -> {
            if (!h.equals(myself))
                sendMessage(message, h);
        });
        lastHeartbeatTime = System.currentTimeMillis();
    }

    private void uponSuspectTimer(SuspectTimer timer, long timerId){    //TODO: check if it's ok to just hardcode the time
        if(!isLeader && System.currentTimeMillis() - lastHeartbeatTime > 2L * heartbeatTimeout) {
            if(electionInProgress){    //Damos um tempo maior até um líder ser escolhido para evitar uma chuva de prepares
                electionInProgress = false; //Na mesma, não podemos bloquear isto para sempre, então desbloqueamos de novo o bucle para garantir liveness em caso de falha do propositor
            }
            else{
                logger.debug("Suspect Time: instance {}", slot_out);
                currentBallot.updateAndIncrement(promisedBallot);
                PrepareMessage message = new PrepareMessage(slot_out, currentBallot);
                membership.forEach(h -> {
                    if (!h.equals(myself))
                        sendMessage(message, h);
                });
                prepareOkResponses.add(myself);
                electionInProgress = true;
            }
        }
    }

    private PaxosInstanceState getOrCreateInstance(int i){  //Tava a fazer o debug e a IA sugeriu isto pra evitar potenciais NullPointerExceptions, parece uma boa observação
        return instances.computeIfAbsent(i,
                k -> new PaxosInstanceState(null, null, null));
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId){
        logger.debug("Received {} from {}", msg, host);
        //should I do anything to reschedule it now?
        if(msg.getOp() != null && membership.contains(host)){
            if(joinedInstance >= 0 ){
                if(msg.getBallot().compareTo(promisedBallot)>=0){
                    electionInProgress = false; //Temos efetivamente um líder
                    lastHeartbeatTime = System.currentTimeMillis();
                    promisedBallot = msg.getBallot(); //O mais provável é ser o mesmo, mas podemos não ter recebido o prepare antes
                    this.currentLeader = host;
                    PaxosInstanceState instance = getOrCreateInstance(msg.getInstance());
                    instance.setAcceptedBallot(msg.getBallot());
                    instance.setAcceptedOpId(msg.getOpId());
                    instance.setAcceptedOperation(msg.getOp());
                    instance.setAcceptOkResponses(new HashSet<>());
                    instance.setMembershipOp(msg.isMembership());
                    AcceptOkMessage message = new AcceptOkMessage(msg.getInstance(), msg.getOpId(), msg.getOp(), msg.getBallot(), msg.isMembership());
                    sendMessage(message, host);
                    slot_in = Math.max(slot_in, msg.getInstance() + 1);
                }
            }
        }
    }

    private void uponAcceptOkMessage(AcceptOkMessage msg, Host host, short sourceProto, int channelId){
        logger.debug("Received {} from {}", msg, host);
        if(joinedInstance >= 0 && membership.contains(host)) {
            if(currentBallot.compareTo(msg.getBallot())==0) {
                PaxosInstanceState instance = getOrCreateInstance(msg.getInstance());
                instance.addAcceptOkResponse(host);
                if(instance.isDecided()){
                    return;
                }
                if (instance.getAcceptOkResponses().size() >= ((membership.size() / 2) + 1)) { //Not sure if this is correct
                    instance.setAcceptedBallot(msg.getBallot());
                    instance.setAcceptedOpId(msg.getOpId());
                    instance.setAcceptedOperation(msg.getOp());
                    instance.setDecided(true);
                    triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
                    DecideMessage message = new DecideMessage(msg.getInstance(), msg.getOpId(), msg.getOp(), msg.getBallot(), msg.isMembership());
                    membership.forEach(h -> {
                        if (!h.equals(myself))
                            sendMessage(message, h);
                    });
                    slot_out = Math.max(slot_out, msg.getInstance() + 1);
                    if(msg.isMembership()){ deserializeAndApplyMembership(msg.getOp()); }
                }
            }
            //senão, ignoramos
        }
    }

    private void uponDecideMessage(DecideMessage msg, Host host, short sourceProto, int channelId){
        logger.debug("Received {} from {}", msg, host);
        if(joinedInstance >= 0 && membership.contains(host)) {
            PaxosInstanceState instance = getOrCreateInstance(msg.getInstance());
            instance.setDecided(true);
            instance.setAcceptedOpId(msg.getOpId());
            instance.setAcceptedOperation(msg.getOp());
            instance.setAcceptedBallot(msg.getBallot());
            instance.setMembershipOp(msg.isMembership());
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
            if(msg.isMembership()){ deserializeAndApplyMembership(msg.getOp()); }
            slot_out = Math.max(slot_out, msg.getInstance() + 1);
            slot_in = Math.max(slot_in, msg.getInstance() + 1);
        }
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, host);
        if(joinedInstance >= 0 && membership.contains(host)) {
            if (msg.getBallot().compareTo(promisedBallot) > 0) {   //Se a request é válida, aceitamos. O líder não reinvindica liderança para evitar disputas
                promisedBallot = msg.getBallot();
                electionInProgress = true; //Esperamos a que um líder seja eleito
                this.isLeader = false;
                Map<Integer, AcceptedValue> acceptedInstances = new HashMap<>();
                for (int i = slot_out; i < slot_in; i++) {  //Não há problema se esse bucle não executar, pode ficar a null
                    PaxosInstanceState instance = getOrCreateInstance(i);
                    if (instance != null && instance.getAcceptedBallot() != null) {
                        AcceptedValue value = new AcceptedValue(instance.getAcceptedBallot(), instance.getAcceptedOpId(), instance.getAcceptedOperation(), instance.isMembershipOp());
                        acceptedInstances.put(i, value);
                    }
                }
                prepareOkResponses.clear();
                PrepareOkMessage message = new PrepareOkMessage(msg.getBallot(), acceptedInstances);
                sendMessage(message, host);
            }
            //Senão, ignoramos a request
        }
    }

    private void uponPrepareOkMessage(PrepareOkMessage msg, Host host, short sourceProto, int channelId){
        logger.debug("Received {} from {}", msg, host);
        if(joinedInstance >= 0 && membership.contains(host)) {
            if(currentBallot.compareTo(msg.getPromised())==0) {
                prepareOkResponses.add(host);
                mergeInstances(msg.getAcceptedInstances());
                if(isLeader) return;
                else if (prepareOkResponses.size() >= ((membership.size() / 2) + 1)) { //We reach quorum
                    isLeader = true;
                    triggerNotification(new LeaderChangeNotification(myself));
                    for(int i = slot_out; i<slot_in; i++) {
                        PaxosInstanceState instance = getOrCreateInstance(i);
                        if(instance.getAcceptedOperation() != null) {
                            AcceptMessage message = new AcceptMessage(i, instances.get(i).getAcceptedOpId(), instances.get(i).getAcceptedOperation(), currentBallot, instances.get(i).isMembershipOp());
                            membership.forEach(h -> {
                                if (!h.equals(myself))
                                    sendMessage(message, h);
                            });
                            instances.get(i).addAcceptOkResponse(myself);
                        }   //maybe we should remove the previous (potentially failed) leader somehow (?)
                    }
                    this.lastHeartbeatTime = System.currentTimeMillis();
                    prepareOkResponses.clear();
                }
            }
            //senão, ignoramos
        }
    }

    private void uponForwardProposalMessage(ForwardProposalMessage msg, Host host, short sourceProto, int channelId){
        logger.debug("Received {} from {}", msg, host);
        if(isLeader && membership.contains(host)){
            PaxosInstanceState instance = new PaxosInstanceState(msg.getOpId(),msg.getOp(),currentBallot, msg.isMembership());
            instances.put(slot_in, instance);
            AcceptMessage message = new AcceptMessage(slot_in, msg.getOpId(), msg.getOp(), currentBallot, msg.isMembership());
            membership.forEach(h -> {
                if (!h.equals(myself))
                    sendMessage(message, h);
            });
            instance.addAcceptOkResponse(myself);
            this.lastHeartbeatTime = System.currentTimeMillis();
            slot_in++;
        }
        else{
            ForwardProposalMessage message = new ForwardProposalMessage(msg.getOpId(), msg.getOp(), msg.isMembership());
            sendMessage(message, currentLeader);
        }
    }

    private void uponHeartbeatMessage(HeartbeatMessage msg, Host host, short sourceProto, int channelId){
        if(membership.contains(host)) {
            this.lastHeartbeatTime = System.currentTimeMillis();
            currentLeader = host;
        }
    }

    private void mergeInstances(Map<Integer, AcceptedValue> values){
        for(int i : values.keySet()){
            AcceptedValue value = values.get(i);
            if(i < slot_in){
                PaxosInstanceState instance = getOrCreateInstance(i);
                if(instance.getAcceptedBallot()==null || instance.getAcceptedBallot().compareTo(value.getBallot()) < 0){
                    instance.setAcceptedBallot(value.getBallot());
                    instance.setAcceptedOpId(value.getOpId());
                    instance.setAcceptedOperation(value.getOp());
                    instance.setAcceptOkResponses(new HashSet<>());
                    instance.setMembershipOp(value.isMembershipOp());
                }
            }
            else{
                PaxosInstanceState instance = new PaxosInstanceState(value.getOpId(), value.getOp(), value.getBallot(), value.isMembershipOp());
                instances.put(i, instance);
            }
            // Atualiza o slot_in apenas com base nas instâncias reais recebidas
            slot_in = Math.max(slot_in, i + 1);
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
        this.promisedBallot = new Ballot(0, myself);
        this.currentBallot = new Ballot(0, myself);
        this.slot_in = joinedInstance;
        this.slot_out = joinedInstance;
        setupPeriodicTimer(new LeaderTimer(), heartbeatTimeout, heartbeatTimeout);
        setupPeriodicTimer(new SuspectTimer(), 2L * heartbeatTimeout, 2L * heartbeatTimeout);
        if(!notification.getMembership().isEmpty()) {
            this.currentLeader = notification.getMembership().get(0); //Pode não ser o líder correto, basta alguém na cadeia conhecer o líder e reenviar. Senão, perdemos alguma request, mas pode ser reenviada.
        }
    }


    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received propose request " + request);
        if(isLeader){
            PaxosInstanceState instance = new PaxosInstanceState(request.getOpId(),request.getOperation(),currentBallot);
            instances.put(slot_in, instance);
            AcceptMessage message = new AcceptMessage(slot_in, request.getOpId(), request.getOperation(), currentBallot, false);
            membership.forEach(h -> {
                if (!h.equals(myself))
                    sendMessage(message, h);
            });
            instance.addAcceptOkResponse(myself);
            this.lastHeartbeatTime = System.currentTimeMillis();
            slot_in++;
        }
        else{
            ForwardProposalMessage msg = new ForwardProposalMessage(request.getOpId(), request.getOperation(), false);
            sendMessage(msg, currentLeader);
        }
    }

    private byte[] serializeMembershipChange(byte type, Host replica) { //pedi pro Gemini
        byte[] ipBytes = replica.getAddress().getAddress();
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(1 + 4 + ipBytes.length + 4);
        buffer.put(type);
        buffer.putInt(ipBytes.length);
        buffer.put(ipBytes);
        buffer.putInt(replica.getPort());
        return buffer.array();
    }

    private void deserializeAndApplyMembership(byte[] op) { //pedi pro Gemini e depois adaptei
        if (op == null || op.length == 0) return;

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(op);
        byte type = buffer.get();
        int ipLen = buffer.getInt();
        byte[] ipBytes = new byte[ipLen];
        buffer.get(ipBytes);
        int port = buffer.getInt();

        try {
            java.net.InetAddress addr = java.net.InetAddress.getByAddress(ipBytes);
            Host targetHost = new Host(addr, port);

            if (type == OP_TYPE_ADD) {
                if (!membership.contains(targetHost)) {
                    membership.add(targetHost);
                    logger.info("Membership updated globally. Added node: " + targetHost);
                }
            } else if (type == OP_TYPE_REMOVE) {
                membership.remove(targetHost);
                logger.info("Membership updated globally. Removed node: " + targetHost);
            }
        } catch (UnknownHostException e) {
            logger.error("Failed to apply dynamic membership consensus transition step", e);
            throw new RuntimeException(e);
        }
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received add replica request " + request);
        ForwardProposalMessage msg = new ForwardProposalMessage(UUID.randomUUID(), serializeMembershipChange(OP_TYPE_ADD, request.getReplica()), true);
        sendMessage(msg, currentLeader);
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received remove replica request " + request);
        ForwardProposalMessage msg = new ForwardProposalMessage(UUID.randomUUID(), serializeMembershipChange(OP_TYPE_REMOVE, request.getReplica()), true);
        sendMessage(msg, currentLeader);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}