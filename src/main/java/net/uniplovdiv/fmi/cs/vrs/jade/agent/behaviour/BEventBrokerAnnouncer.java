package net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour;

import jade.core.behaviours.WakerBehaviour;
import jade.domain.FIPAAgentManagement.Property;
import jade.domain.FIPAAgentManagement.ServiceDescription;
import jade.util.leap.Iterator;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.EventBrokerAgent;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.YellowPagesUtils;

import java.util.Objects;

/**
 * Delayed one-time announcer behaviour informing that a particul.
 * ar agent is an event distributor (broker).
 */
public class BEventBrokerAnnouncer extends WakerBehaviour {
    private static final long serialVersionUID = -177964163377930581L;

    private YellowPagesUtils yup;

    /**
     * Constructor of a Waker behaviour type used to announce a event distribution service via directory facilitator.
     * @param yup Initialized YellowPagesUtils class. Cannot be null.
     * @param eventTopic The event types (category) the associated JADE agent is going to interact with. Cannot be
     *                   null or empty.
     * @throws NullPointerException If yup or eventTopic or the agent instance inside yup is null.
     * @throws IllegalArgumentException If eventTopic is empty.
     */
    public BEventBrokerAnnouncer(YellowPagesUtils yup, String eventTopic) {
        super(yup.getAgent(), 2000L);
        Objects.requireNonNull(eventTopic, "Null event topic");
        if (eventTopic.isEmpty()) throw new IllegalArgumentException("Empty string for eventTopic is not allowed");
        this.yup = yup;

        EventBrokerAgent.registerLangFipaSLIfMissing(yup.getAgent());

        ServiceDescription sd = yup.getServiceDescription();

        for (Iterator it = sd.getAllProperties(); it.hasNext(); ) {
            Property p = (Property) it.next();
            if (p.getName().equals("event-topic")) it.remove();
        }
        sd.addProperties(new Property("event-topic", eventTopic));
    }

    @Override
    protected void onWake() {
        yup.register();
    }
}
