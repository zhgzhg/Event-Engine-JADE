package net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour;

import jade.core.Agent;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

import java.util.Set;
import java.util.LinkedHashSet;

/**
 * Garbage collector behaviour for messages that are not needed / ignored by a particular agent, but still are present
 * in its queue.
 */
public class BMessageGC extends TickerBehaviour {
    private static final long serialVersionUID = -5869596505112699250L;

    private Set<ACLMessage> oldMessages = new LinkedHashSet<>();
    private Set<ACLMessage> newMessages = new LinkedHashSet<>();

    /**
     * Constructor.
     * @param agent The agent this behaviour to be associated with.
     * @param periodMs How often in milliseconds the garbage collection to be run. The longer the period the
     *                 better the performance of the agent however more garbage will accumulate through that time.
     */
    public BMessageGC(Agent agent, long periodMs) {
        super(agent, periodMs);
    }

    @Override
    protected void onTick() {
        final Agent mine = getAgent();
        if (mine.getCurQueueSize() == 0) return;

        ACLMessage msg;
        while ((msg = mine.receive()) != null) {
            if (!oldMessages.contains(msg)) {
                newMessages.add(msg);
            }
        }

        for (ACLMessage m : newMessages) {
            mine.putBack(m);
        }

        oldMessages.clear();
        Set<ACLMessage> tmp = oldMessages;
        oldMessages = newMessages;
        newMessages = tmp;
    }
}
