package protocols.statemachine.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class RetryConnectionTimer extends ProtoTimer {
    public static final short TIMER_ID = 202;
    private final Host target;

    public RetryConnectionTimer(Host target) {
        super(TIMER_ID);
        this.target = target;
    }

    public Host getTarget() {
        return target;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}