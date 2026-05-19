package protocols.agreement.multipaxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class LeaderTimer extends ProtoTimer {
    public static final short TIMER_ID = 102;

    public LeaderTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}