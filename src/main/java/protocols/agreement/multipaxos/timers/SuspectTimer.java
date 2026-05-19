package protocols.agreement.multipaxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class SuspectTimer extends ProtoTimer {
    public static final short TIMER_ID = 101;

    public SuspectTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}