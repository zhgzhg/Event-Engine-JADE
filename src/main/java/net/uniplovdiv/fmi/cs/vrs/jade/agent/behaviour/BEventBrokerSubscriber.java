package net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour;

import jade.content.ContentManager;
import jade.content.onto.basic.Action;
import jade.content.onto.basic.Result;
import jade.core.AID;
import jade.core.Agent;
import jade.core.ContainerID;
import jade.core.behaviours.*;
import jade.domain.FIPANames;
import jade.domain.JADEAgentManagement.JADEManagementOntology;
import jade.domain.JADEAgentManagement.WhereIsAgentAction;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import jade.util.Logger;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.EventBrokerAgent;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.EventEngineOntology;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.SubscriptionParameter;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.YellowPagesUtils;

import java.util.*;
import java.util.logging.Level;

/**
 * Implementation of behaviour subscribing a particular agent for events. Once started it will persists and continuously
 * monitor the availability of the subscription provider and will automatically switch to another one in case of issues.
 * This behaviour contains blocking logic, so it should be wrapped using {@link ThreadedBehaviourFactory}.
 * For e.g. you'll need to have a field with the thread wrapper and should also call interrupt when your agent dies:
 * <pre>
 * <code>
 * public class MyAgent extends Agent {
 *     private ThreadedBehaviourFactory tbf = new ThreadedBehaviourFactory();
 *
 *    {@literal @}Override
 *     protected void setup() {
 *         addBehaviour(
 *             this.tbf.wrap(
 *                 new BEventBrokerSubscriber(
 *                     new YellowPagesUtils(this, ServiceDescriptionUtils.createEventSourceSD(null)),
 *                     null
 *                 )
 *             )
 *         );
 *     }
 *
 *    {@literal @}Override
 *     protected void takeDown() {
 *         try {
 *             ((BEventBrokerSubscriber) this.tbf.getWrappers()[0].getBehaviour()).setDone(true);
 *             ((BEventBrokerSubscriber) this.tbf.getWrappers()[0].getBehaviour()).action();
 *         } catch (NullPointerException e) {}
 *         this.tbf.interrupt();
 *     }
 * }
 * </code>
 * </pre>
 */
public class BEventBrokerSubscriber extends Behaviour {
    private static final long serialVersionUID = -5345319310464473257L;

    private YellowPagesUtils yup;
    private SubscriptionParameter sp;
    private AID chosenEventSourceAgent;
    private boolean hasSubscribed = false;
    private boolean isDone = false;

    private short maxPingAttempts = 5;
    private short currentPingAttempt = 0;
    private long pingTimeoutMs = 200;
    private AID aidToPing = null;

    private boolean blocked = false;
    private long wakeupTime = 0;

    protected static final String PING_PROTOCOL_NAME = "event-engine-ping";
    protected static final String PING_REQUEST = "ping";
    protected static final String PING_RESPONSE = "pong";

    private static MessageTemplate responseAgree;
    private static MessageTemplate responseRefuse;
    private static MessageTemplate responseNotUnderstood;
    private static MessageTemplate acceptedMessages;
    private static MessageTemplate remoteRequestCancelSubscription;

    static {
        responseAgree = MessageTemplate.and(
                MessageTemplate.MatchPerformative(ACLMessage.AGREE),
                MessageTemplate.and(
                        MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                        MessageTemplate.and(
                                MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                                MessageTemplate.MatchProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE)
                        )
                )
        );
        responseRefuse = MessageTemplate.and(
                MessageTemplate.MatchPerformative(ACLMessage.REFUSE),
                MessageTemplate.and(
                        MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                        MessageTemplate.and(
                                MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                                MessageTemplate.MatchProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE)
                        )
                )
        );
        responseNotUnderstood = MessageTemplate.and(
                MessageTemplate.MatchPerformative(ACLMessage.NOT_UNDERSTOOD),
                MessageTemplate.and(
                        MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                        MessageTemplate.and(
                                MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                                MessageTemplate.MatchProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE)
                        )
                )
        );
        acceptedMessages = MessageTemplate.or(responseAgree, MessageTemplate.or(responseRefuse, responseNotUnderstood));
        remoteRequestCancelSubscription = MessageTemplate.and(
                MessageTemplate.MatchPerformative(ACLMessage.CANCEL),
                MessageTemplate.and(
                        MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                        MessageTemplate.and(
                                MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                                MessageTemplate.MatchProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE)
                        )
                )
        );
    }

    /**
     * Constructor.
     * @param yup An initialized instance associated with the agent who will execute the subscription. Cannot be null.
     * @param sp Additional subscription parameters, useful to uniquely identify the current agent and probably to
     *           persist not received event messages sent during a period of agent's absence. Can be null.
     * @throws NullPointerException If yup, or the agent instance inside yup is null.
     */
    public BEventBrokerSubscriber(YellowPagesUtils yup, SubscriptionParameter sp) {
        super(yup.getAgent());
        this.yup = yup;
        this.sp = (sp != null ? sp : new SubscriptionParameter());
        Agent agent = getAgent();
        EventBrokerAgent.registerLangFipaSLIfMissing(agent);

        ContentManager cm = agent.getContentManager();
        cm.registerOntology(EventEngineOntology.getInstance());
        cm.registerOntology(JADEManagementOntology.getInstance());
    }

    /**
     * Retrieves a pseudo-unique container identifier consisting of ContainerID name and port for a particular agent.
     * @param searchAgent The target agent ID to be searched for. Cannot be null
     * @return A nonempty string on success otherwise null.
     * @throws NullPointerException If searchAgent is null or if the failed to retrieve ContainerID parts.
     */
    private String retrieveContainerIdentifierOfAgent(AID searchAgent) {
        Objects.requireNonNull(searchAgent, "Null AID for searchAgent");

        try {
            Agent agent = getAgent();

            ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
            msg.addReceiver(agent.getAMS());
            msg.setOntology(JADEManagementOntology.getInstance().getName());
            msg.setLanguage(FIPANames.ContentLanguage.FIPA_SL);
            msg.setProtocol(FIPANames.InteractionProtocol.FIPA_REQUEST);

            WhereIsAgentAction waa = new WhereIsAgentAction();

            MessageTemplate receiveTemplate = MessageTemplate.and(
                    MessageTemplate.MatchPerformative(ACLMessage.INFORM),
                    MessageTemplate.and(
                            MessageTemplate.MatchOntology(msg.getOntology()),
                            MessageTemplate.and(
                                    MessageTemplate.MatchLanguage(msg.getLanguage()),
                                    MessageTemplate.MatchProtocol(msg.getProtocol())
                            )
                    )
            );

            waa.setAgentIdentifier(searchAgent);
            Action act = new Action(agent.getAMS(), waa);

            msg.setConversationId(UUID.randomUUID().toString());
            MessageTemplate rt = MessageTemplate.and(
                    MessageTemplate.MatchConversationId(msg.getConversationId()),
                    receiveTemplate
            );

            agent.getContentManager().fillContent(msg, act);
            agent.send(msg);

            ACLMessage resp = agent.blockingReceive(rt, 60000);
            if (resp != null) {
                Result r = (Result) agent.getContentManager().extractContent(resp);
                ContainerID cid = (ContainerID) r.getValue();
                return cid.getID() + ":" + cid.getPort();
            }
        } catch (Exception e) {
            Logger.getJADELogger(getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
        }

        return null;
    }

    /**
     * Subscribes to Event Source agent to receive events powered by Event Engine.
     * @param agentId The identifier of the agent.
     * @return True if the subscription was successful, otherwise false.
     */
    private boolean subscribeToEventSource(AID agentId) {
        ACLMessage msg = new ACLMessage(ACLMessage.SUBSCRIBE);
        Agent agent = getAgent();
        msg.setSender(agent.getAID());
        msg.addReceiver(agentId);
        msg.setProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE);
        msg.setLanguage(FIPANames.ContentLanguage.FIPA_SL);
        msg.setOntology(EventEngineOntology.NAME);
        msg.setConversationId(UUID.randomUUID().toString());
        Action act = new Action(agentId, this.sp);
        try {
            agent.getContentManager().fillContent(msg, act);
            agent.send(msg);

            ACLMessage recvMsg = agent.blockingReceive(acceptedMessages, 60000);
            if (recvMsg != null) {
                MessageTemplate ok = MessageTemplate.and(
                        MessageTemplate.MatchSender(agentId),
                        MessageTemplate.and(MessageTemplate.MatchConversationId(msg.getConversationId()), responseAgree)
                );
                return ok.match(recvMsg);
            }
        } catch (Exception e) {
            Logger.getJADELogger(getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
        }

        return false;
    }

    /**
     * Unsubscribes from Event Source agent in order not to receive events.
     * @param agentId The identifier of the agent.
     */
    protected void unsubscribeFromEventSource(AID agentId) {
        Agent agent = getAgent();
        ACLMessage msg = new ACLMessage(ACLMessage.CANCEL);
        msg.setSender(agent.getAID());
        msg.addReceiver(agentId);
        msg.setConversationId(UUID.randomUUID().toString());
        msg.setOntology(EventEngineOntology.NAME);
        msg.setLanguage(FIPANames.ContentLanguage.FIPA_SL);
        msg.setProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE);
        Action act = new Action(agentId, this.sp);
        try {
            agent.getContentManager().fillContent(msg, act);
            agent.send(msg);
        } catch (Exception e) {
            Logger.getJADELogger(getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * Checks for any remote requests from EventBroker agents to the current agent to stop expecting events from them
     * because they will be suspending, terminating or for another reason.
     * @return True if such request exists, otherwise false.
     */
    protected boolean checkAndExecuteRemoteUnsubscribeRequests() {
        AID sender = getChosenEventSourceAgent();
        if (sender != null) {
            MessageTemplate mt = MessageTemplate.and(MessageTemplate.MatchSender(sender),
                    remoteRequestCancelSubscription);
            ACLMessage msg = getAgent().receive(mt);
            if (msg != null) {
                this.hasSubscribed = false;
                setChosenEventSourceAgent(null);
                return true;
            }
        }
        return false;
    }

    @Override
    public void action() {
        if (isDone) {
            this.blocked = false;
            if (this.hasSubscribed) {
                AID eSrc = getChosenEventSourceAgent();
                unsubscribeFromEventSource(eSrc);
            }
            return;
        }

        if (blocked) {
            if (this.hasSubscribed) {
                // although we're in blocking check if there're any messages announcing that the event source agent
                // stops distributing events to us
                checkAndExecuteRemoteUnsubscribeRequests();
            }

            long blockTime = this.wakeupTime - System.currentTimeMillis();
            if (blockTime > 0L) {
                this.blocked = true;
                this.block(blockTime);
                return;
            } else {
                this.blocked = false;
            }
        }

        if (!this.hasSubscribed) {
            this.subscribeToEventSource();

            if (!this.hasSubscribed) {
                this.blocked = true;
                this.wakeupTime = System.currentTimeMillis() + 20000L;
                block(this.wakeupTime);
            }
        } else { // check whether the subscribed agent is alive
            if (checkAndExecuteRemoteUnsubscribeRequests()) {
                this.blocked = true;
                this.wakeupTime = System.currentTimeMillis() + 20000L;
                block(this.wakeupTime);
                return;
            }

            AID eSrc = getChosenEventSourceAgent();
            if (retrieveContainerIdentifierOfAgent(eSrc) == null || !pingAgent(eSrc)) {
                // the agent is probably dead. Just in case try sending to it an unsubscribe message.
                this.hasSubscribed = false;
                unsubscribeFromEventSource(eSrc);
            } else {
                // the agent is alive. Check once again after a minute.
                this.blocked = true;
                this.wakeupTime = System.currentTimeMillis() + 60000L;
                block(this.wakeupTime);
            }
        }
    }

    /**
     * Searches for event source agents and tries to subscribe to one of them.
     */
    protected void subscribeToEventSource() {
        // do not attempt subscription if we are (entering) in suspended or deleted state
        int state = getAgent().getAgentState().getValue();
        if (state == Agent.AP_SUSPENDED || state == Agent.AP_DELETED) {
            return;
        }

        Agent agent = getAgent();

        List<AID> agents = this.yup.search();

        if (!agents.isEmpty()) {
            final AID myAID = agent.getAID();

            // The current agent is broker too. No need to execute more queries since we mustn't subscribe to ourselves.
            if (agents.size() == 1 && agents.get(0).equals(myAID)) {
                return;
            }

            String myContainerIdentifier = this.retrieveContainerIdentifierOfAgent(myAID);

            final List<AID> subscriptionAlternatives = new ArrayList<>();

            for (AID a : agents) {
                if (a.equals(myAID)) continue;

                String contIdentifier = this.retrieveContainerIdentifierOfAgent(a);

                // prefer agent within the same container if possible
                if (myContainerIdentifier == null || myContainerIdentifier.equals(contIdentifier)) {
                    if (this.hasSubscribed = subscribeToEventSource(a)) {
                        setChosenEventSourceAgent(a);
                        break;
                    }
                } else {
                    subscriptionAlternatives.add(a);
                }

            }

            if (!hasSubscribed && !subscriptionAlternatives.isEmpty()) {
                for (AID sa : subscriptionAlternatives) {
                    if (this.hasSubscribed = subscribeToEventSource(sa)) {
                        setChosenEventSourceAgent(sa);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Sets the AID of the chosen event source agent.
     * @param aid The agent id to be chosen.
     */
    protected synchronized void setChosenEventSourceAgent(AID aid) {
        this.chosenEventSourceAgent = aid;
    }

    /**
     * Returns the AID of the chosen event source agent (if any).
     * @return An AID instance or a null if nothing has been chosen.
     */
    public synchronized AID getChosenEventSourceAgent() {
        return this.chosenEventSourceAgent;
    }

    /**
     * Checks if an agent is responding to our requests by pinging it. This method is blocking the execution.
     * @param aid The agent's identifier to check. Should not be null. Changing the aid will reset method's ping attempt
     *            counters (if the aid is not null).
     * @return True if the agent responds otherwise false.
     */
    protected boolean pingAgent(AID aid) {
        if (aid == null) return false;

        Agent agent = getAgent();
        if (aid.equals(agent.getAID())) return true;

        // If we're (entering) in suspended or deleted state do not execute ping
        int state = agent.getAgentState().getValue();
        if (state == Agent.AP_SUSPENDED || state == Agent.AP_DELETED) {
            return false;
        }

        if (this.aidToPing != aid || !aid.equals(this.aidToPing)) {
            this.currentPingAttempt = 0;
            this.aidToPing = aid;
        }

        if (this.currentPingAttempt++ < this.maxPingAttempts) {
            ACLMessage ping = new ACLMessage(ACLMessage.REQUEST);
            ping.addReceiver(aid);
            ping.setLanguage(FIPANames.ContentLanguage.FIPA_SL);
            ping.setProtocol(PING_PROTOCOL_NAME);
            ping.setContent(PING_REQUEST);
            ping.addUserDefinedParameter("ping-time-ms", Long.toString(System.currentTimeMillis()));

            MessageTemplate ok = MessageTemplate.and(
                    MessageTemplate.and(
                            MessageTemplate.MatchPerformative(ACLMessage.INFORM),
                            MessageTemplate.and(
                                    MessageTemplate.MatchProtocol(PING_PROTOCOL_NAME),
                                    MessageTemplate.MatchContent(PING_RESPONSE)
                            )
                    ),
                    MessageTemplate.and(
                            MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                            MessageTemplate.MatchSender(aid)
                    )
            );
            agent.send(ping);

            ACLMessage pong = agent.blockingReceive(ok, this.pingTimeoutMs);
            if (pong != null) {
                long start, end;
                try {
                    start = Long.parseLong(pong.getUserDefinedParameter("ping-time-ms"));
                    end = Long.parseLong(pong.getUserDefinedParameter("pong-time-ms"));
                } catch (Exception e) {
                    start = 0;
                    end = 0;
                }
                if (start != 0 && end != 0 && Math.abs(end - start) <= (this.pingTimeoutMs * 3)) {
                    this.currentPingAttempt = 0;
                }
            }
            return true;
        }

        return false;
    }

    /**
     * Returns agent's max retries count for pinging another agent.
     * @return A positive number.
     */
    public short getMaxPingAttempts() {
        return maxPingAttempts;
    }

    /**
     * Sets agent's max retries count for pinging another agent.
     * @param maxPingAttempts A positive number (greater than 0).
     * @throws IllegalArgumentException If maxPingAttempts is less than 1.
     */
    public void setMaxPingAttempts(short maxPingAttempts) {
        if (maxPingAttempts < 1) {
            throw new IllegalArgumentException("Argument maxPingAttempts must be greater than 0");
        }
        this.maxPingAttempts = maxPingAttempts;
    }

    /**
     * Returns the maximum time to wait for response during ping.
     * @return A positive number (greater than 0) describing the time in milliseconds.
     */
    public long getPingTimeoutMs() {
        return this.pingTimeoutMs;
    }

    /**
     * Sets the maximum time to wait for response during ping.
     * @param pingTimeoutMs A positive number (greater than 0) describing the time in milliseconds.
     * @throws IllegalArgumentException If pingTimeoutMs is less than 1.
     */
    public void setPingIntervalMs(long pingTimeoutMs) {
        this.pingTimeoutMs = pingTimeoutMs;
    }

    @Override
    public boolean done() {
        return isDone;
    }

    /**
     * Checks whether the behaviour is marked as done.
     * @return True if the behaviour is done, otherwise false.
     */
    public boolean isDone() {
        return isDone;
    }

    /**
     * Changes the "done" state of the behaviour. Once set to done it will be stopped from executing.
     * @param done Set to true to designate done behaviour or false to not done yet.
     */
    public void setDone(boolean done) {
        isDone = done;
    }
}
