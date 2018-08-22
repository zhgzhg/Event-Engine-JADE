package net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour;

import jade.core.behaviours.WakerBehaviour;
import jade.domain.FIPAAgentManagement.Property;
import jade.domain.FIPAAgentManagement.ServiceDescription;
import jade.util.leap.Iterator;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.EventBrokerAgent;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.configuration.BasicConfiguration;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.ServiceDescriptionUtils;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.YellowPagesUtils;

import java.util.Objects;

/**
 * Delayed one-time announcer behaviour informing that a particul.
 * ar agent is an event distributor (broker).
 */
public class BEventBrokerAnnouncer extends WakerBehaviour {
    private static final long serialVersionUID = -1373575430029846294L;

    private YellowPagesUtils yup;
    private BasicConfiguration brokerCfg;

    /**
     * Constructor of a Waker behaviour type used to announce a event distribution service via directory facilitator.
     * @param yup Initialized YellowPagesUtils class. Cannot be null.
     * @param brokerCfg The configuration used for initializing a particular broker instance.
     * @throws NullPointerException If yup or eventTopic or the agent instance inside yup is null.
     * @throws IllegalArgumentException If eventTopic is empty.
     */
    public BEventBrokerAnnouncer(YellowPagesUtils yup, BasicConfiguration brokerCfg) {
        super(yup.getAgent(), 2000L);
        Objects.requireNonNull(brokerCfg, "Null broker configuration provided");
        if (brokerCfg.topic.isEmpty())
            throw new IllegalArgumentException("Empty string for topic in brokerCfg is not allowed");
        this.yup = yup;
        this.brokerCfg = brokerCfg;

        EventBrokerAgent.registerLangFipaSLIfMissing(yup.getAgent());
    }

    @Override
    protected void onWake() {
        if (this.brokerCfg.topic.isEmpty())
            throw new IllegalArgumentException("Empty string for topic in brokerCfg is not allowed");

        ServiceDescription sd = this.yup.getServiceDescription();

        ServiceDescriptionUtils.setFirstPropertyNamed(
                ServiceDescriptionUtils.EVENT_TOPIC_PROP, this.brokerCfg.topic, sd);

        ServiceDescriptionUtils.setFirstPropertyNamed(
                ServiceDescriptionUtils.MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS_PROP,
                this.brokerCfg.maxTimeWithoutBrokerConnectionMillis, sd);

        this.yup.register();
    }
}
