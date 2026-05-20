package protocols.agreement.raft.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ElectionTimer extends ProtoTimer {
    public static final short TIMER_ID = 1;
    public ElectionTimer() { super(TIMER_ID); }
    @Override public ProtoTimer clone() { return this; }
}