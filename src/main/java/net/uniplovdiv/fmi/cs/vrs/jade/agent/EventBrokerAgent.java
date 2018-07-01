package net.uniplovdiv.fmi.cs.vrs.jade.agent;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;
import jade.content.ContentElement;
import jade.content.lang.sl.SLCodec;
import jade.content.onto.basic.Action;
import jade.core.AID;
import jade.core.Agent;

import jade.core.Location;
import jade.core.behaviours.*;
import jade.domain.FIPAAgentManagement.*;
import jade.domain.FIPANames;
import jade.domain.JADEAgentManagement.JADEManagementOntology;
import jade.domain.JADEAgentManagement.WhereIsAgentAction;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import jade.proto.SubscriptionResponder;
import jade.util.Logger;

import net.uniplovdiv.fmi.cs.vrs.event.Event;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.CircularFifoSet;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.IEventDispatcher;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.AbstractEventDispatcher;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.activemq.EventDispatcherActiveMQ;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.kafka.EventDispatcherKafka;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour.BEventBrokerAnnouncer;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour.BEventBrokerSubscriber;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour.BEventChannel;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour.BMessageGC;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.configuration.BasicConfiguration;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.EventEngineOntology;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.SubscriptionParameter;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.ServiceDescriptionUtils;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.YellowPagesUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Implementation of Event Engine's events distributor agent. The agent works as a "proxy" associating for every
 * subscriber real connections to concrete broker systems and then relaying messages from and to those systems to the
 * subscriber agent and vice versa. Because of this approach a limitation on the number of subscribers is usually set.
 * The exact number depends of how many broker systems the broker agent will have to communicate with.
 */
public class EventBrokerAgent extends Agent {
    private static final long serialVersionUID = -4431769633058927649L;

    protected final Logger logger = Logger.getJADELogger(getClass().getName());

    protected final ConcurrentMap<AID, SubscriberData> agentToIdForBrokerMap = new ConcurrentHashMap<>();

    protected String eventTopic = new Event().getCategory();
    protected YellowPagesUtils yup =
            new YellowPagesUtils(this, ServiceDescriptionUtils.createEventSourceSD(eventTopic));
    protected BSubscriptionResponder bSubscriptionResponder = null;
    protected BPingResponder bPingResponder = null;
    protected BEventBrokerAnnouncer bEventBrokerAnnouncer = null;
    protected BEventChannel bEventChannel = null;
    protected BasicConfiguration configuration = null;
    protected ScheduledExecutorService taskScheduler = null;

    protected Function<SubscriptionParameter, Consumer<BasicConfiguration.Link>> configurationRemapper = (sp) -> {
        return (lnk) -> {
            if (lnk == null || (sp.getPersonalId() == null && sp.getPersonalId2() == null)) return;

            Properties cfgData = lnk.getConfigurationData();
            Class<? extends AbstractEventDispatcher> aed = lnk.getAbstractEventDispatcher();

            // no point in refactoring for now

            if (EventDispatcherKafka.class.isAssignableFrom(aed)) {
                String v = sp.getPersonalId();
                if (v != null && !v.isEmpty()) {
                    String k = "client.id";
                    if (cfgData.containsKey(k)) cfgData.setProperty(k, v);
                }

                v = sp.getPersonalId2();
                if (v != null && !v.isEmpty()) {
                    String k = "group.id";
                    if (cfgData.containsKey(k)) cfgData.setProperty(k, v);
                }

            } else if (EventDispatcherActiveMQ.class.isAssignableFrom(aed)) {
                String v = sp.getPersonalId();
                if (v != null && !v.isEmpty()) {
                    String k = "jms.clientID";
                    if (cfgData.containsKey(k)) cfgData.setProperty(k, v);
                }
            }

            /*String v = sp.getPersonalId(); // older version. just to remind how a better written form looks like
            if (!v.isEmpty()) {
                String k = null;
                if (EventDispatcherKafka.class.isAssignableFrom(aed)) {
                    k = "client.id";
                } else if (EventDispatcherActiveMQ.class.isAssignableFrom(aed)) {
                    k = "jms.clientID";
                }
                if (k != null && lnk.getConfigurationData().containsKey(k)) {
                    cfgData.setProperty(k, v);
                }
            }*/

        };
    };


    /**
     * Holder for initialized broker connections and corresponding setting parameters.
     */
    public static class SubscriberData {
        private SubscriptionParameter subscriptionParameter;
        private volatile Collection<String> latestDeduplicationKeys;
        private volatile List<IEventDispatcher> connections;
        private volatile boolean allowConnections;

        /**
         * Constructor.
         * @param deduplicationCapacity Capacity of the memory used for deduplication of events arriving from multiple
         *                              broker systems. The minimum recommended value is the total sum of the
         *                              "latestEventsRememberCapacity" for all broker links, multiplied by the number of
         *                              links. Must be greater than 0.
         * @throws IllegalArgumentException If deduplicationCapacity is not greater than 0.
         */
        public SubscriberData(int deduplicationCapacity) {
            this.allowConnections = true;
            this.latestDeduplicationKeys =
                    Collections.synchronizedCollection(new CircularFifoSet<>(deduplicationCapacity));
        }

        /**
         * Constructor.
         * @param sp The subscription parameter to be set.
         * @param deduplicationCapacity Capacity of the memory used for deduplication of events arriving from multiple
         *                              broker systems. The minimum recommended value is the total sum of the
         *                              "latestEventsRememberCapacity" for all broker links, multiplied by the number of
         *                              links. Must be greater than 0.
         * @throws IllegalArgumentException If deduplicationCapacity is not greater than 0.
         */
        public SubscriberData(SubscriptionParameter sp, int deduplicationCapacity) {
            this(deduplicationCapacity);
            this.subscriptionParameter = sp;
        }

        /**
         * Attempts to synchronously set the connections field of the instance.
         * @param connections The value to be set.
         * @return True if changes are permitted and the setting is complete, otherwise false.
         */
        public synchronized boolean setConnections(List<IEventDispatcher> connections) {
            if (this.allowConnections) {
                this.connections = connections;
                return true;
            }
            return false;
        }

        /**
         * Returns the current dispatcher systems connections in the instance.
         * @return A list that might contain 0 or more entries or a null.
         */
        public synchronized List<IEventDispatcher> getConnections() {
            return connections;
        }

        /**
         * Returns the current subscription parameter associated with the instance.
         * @return A {@link SubscriptionParameter} instance or null.
         */
        public SubscriptionParameter getSubscriptionParameter() {
            return subscriptionParameter;
        }

        /**
         * Sets the current subscription parameter associated with the instance.
         * @param subscriptionParameter The subscription parameter instance to be set.
         */
        public void setSubscriptionParameter(SubscriptionParameter subscriptionParameter) {
            this.subscriptionParameter = subscriptionParameter;
        }

        /**
         * Checks whether creating new connections is allowed.
         * @return True if allowed, otherwise false.
         */
        public synchronized boolean isAllowConnections() {
            return allowConnections;
        }

        /**
         * Changes the permissions regarding creation of new connections.
         * @param allowConnections Set to true to allow new connections, or to false to disallow them.
         */
        public synchronized void setAllowConnections(boolean allowConnections) {
            this.allowConnections = allowConnections;
        }

        /**
         * Locks the modification of the connections and iterates over them applying a callback.
         * @param callback The code to be applied over every connection member.
         */
        public synchronized void iterateOverConnections(Consumer<IEventDispatcher> callback) {
            if (connections != null && !connections.isEmpty() && callback != null) {
                connections.forEach(callback);
            }
        }

        /**
         * Returns the structure used for remembering event duplication keys.
         * @return A synchronized collection.
         */
        public synchronized Collection<String> getLatestDeduplicationKeys() {
            return latestDeduplicationKeys;
        }

        /**
         * Sets the structure used for remembering event duplication keys.
         * @param latestDeduplicationKeys A non thread-safe collection that will be internally converted to synchronized
         *                                one. Cannot be null.
         * @throws NullPointerException If latestDeduplicationKeys is null.
         */
        public synchronized void setLatestDeduplicationKeys(Collection<String> latestDeduplicationKeys) {
            this.latestDeduplicationKeys = Collections.synchronizedCollection(latestDeduplicationKeys);
        }
    }

    /**
     * Responder for other agent's subscription requests.
     */
    protected static class BSubscriptionResponder extends SubscriptionResponder {
        private static final long serialVersionUID = -6017225801802881625L;
        private static MessageTemplate subsRequest;
        private static MessageTemplate unsubsRequest;
        private static MessageTemplate acceptedMessages;

        static {
            subsRequest = MessageTemplate.and(
                    MessageTemplate.MatchPerformative(ACLMessage.SUBSCRIBE),
                    MessageTemplate.and(
                            MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                            MessageTemplate.and(
                                    MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                                    MessageTemplate.MatchProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE)
                            )
                    )
            );
            unsubsRequest = MessageTemplate.and(
                    MessageTemplate.MatchPerformative(ACLMessage.CANCEL),
                    MessageTemplate.and(
                            MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                            MessageTemplate.and(
                                    MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                                    MessageTemplate.MatchProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE)
                            )
                    )
            );
            acceptedMessages = MessageTemplate.or(subsRequest, unsubsRequest);
        }

        /**
         * Constructor.
         * @param agent The EventBrokerAgent to associate the responder with. Cannot be null.
         */
        public BSubscriptionResponder(EventBrokerAgent agent) {
            super(agent, acceptedMessages);
        }

        @Override
        protected ACLMessage handleCancel(ACLMessage cancel) throws FailureException {
            ACLMessage res = super.handleCancel(cancel); // by default null

            AID sender = cancel.getSender();
            if (sender == null) return res;

            EventBrokerAgent agent = (EventBrokerAgent) getAgent();
            agent.unsubscribeAIDForEvents(sender);

            return res;
        }

        @Override
        protected ACLMessage handleSubscription(ACLMessage subscription) {
            ACLMessage reply = subscription.createReply();
            AID sender = subscription.getSender();

            EventBrokerAgent agent = (EventBrokerAgent) getAgent();

            if (agent.isAIDSubscribed(sender)) {
                reply.setPerformative(ACLMessage.AGREE);
            } else {
                SubscriptionParameter sp;
                try {
                    ContentElement ce = agent.getContentManager().extractContent(subscription);
                    Action act;
                    if (ce instanceof Action
                            && (((act = ((Action) ce)).getAction()) instanceof SubscriptionParameter)) {
                        sp = (SubscriptionParameter) act.getAction();

                        if (sp == null) {
                            sp = new SubscriptionParameter(UUID.randomUUID().toString(),
                                    UUID.randomUUID().toString(), null);
                        } else {
                            if (sp.getPersonalId() == null || sp.getPersonalId().isEmpty())
                                sp.setPersonalId(UUID.randomUUID().toString());
                            if (sp.getPersonalId2() == null || sp.getPersonalId2().isEmpty())
                                sp.setPersonalId2(UUID.randomUUID().toString());
                        }

                        agent.subscribeAIDForEvents(sender, sp,
                                (agent.configuration.maxTimeWithoutBrokerConnectionMillis > 0
                                        ?
                                        agent.configuration.maxTimeWithoutBrokerConnectionMillis
                                        :
                                        BasicConfiguration.DEFAULT_MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS
                                ),
                                () -> {
                                    try {
                                        super.handleSubscription(subscription);
                                        reply.setPerformative(ACLMessage.AGREE);
                                    } catch (Exception ex) {
                                        reply.setPerformative(ACLMessage.REFUSE);
                                    }
                                    agent.send(reply);
                                },
                                () -> {
                                    reply.setPerformative(ACLMessage.REFUSE);
                                    agent.send(reply);
                                }
                        );

                        /*if (agent.subscribeAIDForEvents(sender, sp)) {
                            super.handleSubscription(subscription);
                            reply.setPerformative(ACLMessage.AGREE);
                        } else {
                            reply.setPerformative(ACLMessage.REFUSE);
                        }*/
                    }
                } catch (Exception e) {
                    Logger.getJADELogger(this.getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
                    reply.setPerformative(ACLMessage.REFUSE);
                    return reply;
                }
            }

            //return reply;
            return null;
        }

        /**
         * Informs the receiver that has been remotely unsubscribed.
         * @param receivers The agent id to inform.
         * @param sender The agent to send the message.
         */
        public static void sendUnsubscribedRemotelyMsg(Agent sender, AID... receivers) {
            if (sender != null && receivers != null && receivers.length > 0) {
                ACLMessage unsubscrMsg = new ACLMessage(ACLMessage.CANCEL);
                unsubscrMsg.setOntology(EventEngineOntology.NAME);
                unsubscrMsg.setLanguage(FIPANames.ContentLanguage.FIPA_SL);
                unsubscrMsg.setProtocol(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE);
                unsubscrMsg.setSender(sender.getAID());
                for (AID r : receivers) {
                    if (r != null) {
                        unsubscrMsg.addReceiver(r);
                    }
                }
                sender.send(unsubscrMsg);
            }
        }

        /**
         * Performs local unsubscribe action removing the corresponding AID from the register and informs the remote
         * side (subscriber for that).
         * @param subscriberAids The agent identifier of the subscriber to unsubscribe and inform.
         */
        public void remoteUnsubscribe(AID... subscriberAids) {
            if (subscriberAids != null && subscriberAids.length > 0) {
                EventBrokerAgent agent = (EventBrokerAgent) getAgent();
                sendUnsubscribedRemotelyMsg(agent, subscriberAids);
                for (AID s : subscriberAids) {
                    if (s != null) {
                        agent.unsubscribeAIDForEvents(s);
                    }
                }
            }
        }
    }

    /**
     * Responder for other agent's ping requests checking if the current agent is responsive.
     */
    protected static class BPingResponder extends CyclicBehaviour {
        private static final long serialVersionUID = -2488878343947109996L;
        /**
         * The name of the used protocol.
         */
        protected static final String PING_PROTOCOL_NAME = "event-engine-ping";
        /**
         * The expected content value during request.
         */
        protected static final String PING_REQUEST = "ping";
        /**
         * The expected content value during response.
         */
        protected static final String PING_RESPONSE = "pong";

        private int maxRequestAnswerAtOnce;
        private MessageTemplate pingRequestTemplate = MessageTemplate.and(
                MessageTemplate.and(
                        MessageTemplate.MatchPerformative(ACLMessage.REQUEST),
                        MessageTemplate.MatchProtocol(PING_PROTOCOL_NAME)
                ),
                MessageTemplate.and(
                        MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
                        MessageTemplate.MatchContent(PING_REQUEST)
                )
        );

        /**
         * Constructor.
         * @param agent The agent with which to associate the behaviour.
         * @param maxRequestAnswerAtOnce The maximum number of ping requests to answer outright
         */
        public BPingResponder(Agent agent, int maxRequestAnswerAtOnce) {
            super(agent);
            if (maxRequestAnswerAtOnce <= 0) {
                this.maxRequestAnswerAtOnce = 1;
            } else {
                this.maxRequestAnswerAtOnce = maxRequestAnswerAtOnce;
            }
        }

        @Override
        public void action() {
            Agent a = this.getAgent();
            for (int i = 0; i < this.maxRequestAnswerAtOnce; ++i) {
                ACLMessage msg = a.receive(pingRequestTemplate);
                if (msg != null) {
                    ACLMessage reply = msg.createReply();
                    reply.setPerformative(ACLMessage.INFORM);
                    reply.setContent(PING_RESPONSE);
                    reply.setInReplyTo(Long.toString(msg.getPostTimeStamp()));
                    reply.addUserDefinedParameter("ping-time-ms", msg.getUserDefinedParameter("ping-time-ms"));
                    reply.addUserDefinedParameter("pong-time-ms", Long.toString(System.currentTimeMillis()));
                    a.send(reply);
                } else {
                    break;
                }
            }
            block();
        }
    }

    /**
     * Behaviour user to check if the subscribers of the agent still exist.
     */
    protected static class BSubscriberExistsChecker extends TickerBehaviour {
        private static final long serialVersionUID = 3993707730681008403L;

        private EventBrokerAgent agent;
        private Consumer<AID> onMiss;
        private int maxRetries = 5;
        private HashMap<AID, AgentRequest> aidRetriesMap = new HashMap<>();
        private HashMap<String, AID> conversationIdAidMap = new HashMap<>();

        /**
         * Used to store agent subscribers and communication requests with AMS regarding them.
         */
        private static class AgentRequest {
            public int attempt;
            public String conversationId;

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                AgentRequest that = (AgentRequest) o;
                return attempt == that.attempt &&
                        Objects.equals(conversationId, that.conversationId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(attempt, conversationId);
            }
        }

        /**
         * Constructor.
         * @param agent The event broker agent to associate this behaviour with.
         * @param periodMs How often in milliseconds the checks to occur.
         * @param onMiss The consumer to be executed if the checked agent is missing.
         * @throws NullPointerException If any of the parameters are null.
         */
        public BSubscriberExistsChecker(EventBrokerAgent agent, long periodMs, Consumer<AID> onMiss) {
            super(agent, periodMs);
            Objects.requireNonNull(agent);
            Objects.requireNonNull(onMiss);
            this.agent = agent;
            this.onMiss = onMiss;
        }

        @Override
        public void onTick() {
            if (agent.subscribersCount() < 1) {
                if (aidRetriesMap.isEmpty()) {
                    aidRetriesMap.clear();
                    conversationIdAidMap.clear();
                }
                return;
            }

            Collection<AID> subscribers = this.agent.getSubscribedAIDs();
            aidRetriesMap.entrySet().retainAll(subscribers);

            ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
            msg.addReceiver(agent.getAMS());
            msg.setOntology(JADEManagementOntology.getInstance().getName());
            msg.setLanguage(FIPANames.ContentLanguage.FIPA_SL);
            msg.setProtocol(FIPANames.InteractionProtocol.FIPA_REQUEST);

            MessageTemplate receiveTemplate = MessageTemplate.and(
                    MessageTemplate.or(
                            MessageTemplate.MatchPerformative(ACLMessage.INFORM),
                            MessageTemplate.MatchPerformative(ACLMessage.FAILURE)
                    ),
                    MessageTemplate.and(
                            MessageTemplate.and(
                                    MessageTemplate.MatchOntology(msg.getOntology()),
                                    MessageTemplate.MatchSender(agent.getAMS())
                            ),
                            MessageTemplate.and(
                                    MessageTemplate.MatchLanguage(msg.getLanguage()),
                                    MessageTemplate.MatchProtocol(msg.getProtocol())
                            )
                    )
            );

            WhereIsAgentAction waa = new WhereIsAgentAction();
            Action act = new Action(agent.getAMS(), waa);

            subscribers.forEach(searchAgent -> {
                AgentRequest aqStat = aidRetriesMap.get(searchAgent);
                if (aqStat != null && aqStat.attempt > this.maxRetries) {
                    this.aidRetriesMap.remove(searchAgent);
                    this.conversationIdAidMap.remove(aqStat.conversationId);
                    onMiss.accept(searchAgent);
                    return;
                }

                String convId = (aqStat == null ? UUID.randomUUID().toString() : aqStat.conversationId);
                MessageTemplate rt = MessageTemplate.and(MessageTemplate.MatchConversationId(convId), receiveTemplate);

                if (aqStat == null) {
                    try {
                        waa.setAgentIdentifier(searchAgent);
                        msg.setConversationId(convId);
                        agent.getContentManager().fillContent(msg, act);
                        agent.send(msg);
                    } catch (Exception e) {
                        onMiss.accept(searchAgent);
                        Logger.getJADELogger(getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
                        return;
                    }
                }

                ACLMessage recv = agent.receive(rt);
                if (recv == null) {
                    if (aqStat == null) {
                        aqStat = new AgentRequest();
                        aqStat.attempt = 0;
                        aqStat.conversationId = convId;
                        this.aidRetriesMap.put(searchAgent, aqStat);
                        this.conversationIdAidMap.put(aqStat.conversationId, searchAgent);
                    }
                }
            });

            if (!this.aidRetriesMap.isEmpty()) {
                ArrayList<ACLMessage> toRestore = new ArrayList<>(this.aidRetriesMap.size());

                ACLMessage recv;
                while ((recv = agent.receive(receiveTemplate)) != null) {
                    String convId = recv.getConversationId();
                    AID aid = this.conversationIdAidMap.get(convId);
                    if (aid == null) {
                        toRestore.add(recv);
                    } else {
                        this.aidRetriesMap.remove(aid);
                        this.conversationIdAidMap.remove(convId);
                        if (recv.getPerformative() == ACLMessage.FAILURE) {
                            onMiss.accept(aid);
                        }
                    }
                }
                toRestore.forEach(_msg -> agent.putBack(_msg));

                // iterate the remaining entries - for them no response has been received
                for (Iterator<AgentRequest> it = this.aidRetriesMap.values().iterator(); it.hasNext(); ) {
                    AgentRequest ar = it.next();
                    if (ar.attempt < this.maxRetries) {
                        ++ar.attempt;
                    } else {
                        it.remove();
                        AID aid = this.conversationIdAidMap.remove(ar.conversationId);
                        onMiss.accept(aid);
                    }
                }
            }
        }
    }

    /**
     * Constructor.
     */
    public EventBrokerAgent() { super(); }

    /**
     * Constructor allowing to specify different remapping function for the parameters provided by any subscriber
     * agents when initiating subscription. The parameters may include broker settings and other stuff...
     * @param configurationRemapper A data remapping function. If it's set to null no remapping will be done.
     *                              This class also implements a default remapping function. If its use is desired
     *                              then you should use the default constructor of this class.
     */
    public EventBrokerAgent(Function<SubscriptionParameter, Consumer<BasicConfiguration.Link>> configurationRemapper) {
        super();
        this.configurationRemapper = configurationRemapper;
    }

    @Override
    protected void setup() {
        Object[] arguments = getArguments();
        String filePath = "dispatcher.properties";

        if (arguments != null && arguments.length != 0) {
            final String cfgArg = "--config=";
            for (Object arg : arguments) {
                String s = arg.toString();
                if (s.length() > cfgArg.length() && s.startsWith(cfgArg)) {
                    filePath = s.substring(cfgArg.length());
                }
            }
        }

        try {
            Properties p = new Properties();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                p.load(fis);
            }
            JavaPropsSchema schema = JavaPropsSchema.emptySchema().withPathSeparator("->")
                    .withWriteIndexUsingMarkers(true).withFirstArrayOffset(0).withParseSimpleIndexes(false);
            JavaPropsMapper mapper = new JavaPropsMapper();
            this.configuration = mapper.readPropertiesAs(p, schema, BasicConfiguration.class);
            if (this.configuration.maxSubscribersLimit <= 0) {
                this.configuration.maxSubscribersLimit = 20;
            }
            int scheduledPoolSz = (this.configuration.link != null
                    && this.configuration.link.size() > 6 ? this.configuration.link.size() : 6);

            this.taskScheduler = Executors.newScheduledThreadPool(
                    Math.max(6, Math.min(scheduledPoolSz, Runtime.getRuntime().availableProcessors())),
                    new BasicThreadFactory.Builder()
                            .namingPattern("event-engine-jade-scheduled-tp-%d")
                            .daemon(true)
                            .priority(Thread.MAX_PRIORITY)
                            .build()
            );
            ((ScheduledThreadPoolExecutor)this.taskScheduler).setRemoveOnCancelPolicy(true);

        } catch (Exception e) {
            logger.log(Logger.SEVERE, e.getMessage(), e);
            logger.info("You can specify the configuration file by passing argument --config=<file_path>");
            logger.info("The current working directory is: "
                    + Paths.get(".").toAbsolutePath().normalize().toString());
            this.doDelete();
            return;
        }

        registerLangFipaSLIfMissing(this);
        this.getContentManager().registerOntology(EventEngineOntology.getInstance());
        this.getContentManager().registerOntology(FIPAManagementOntology.getInstance());
        this.getContentManager().registerOntology(JADEManagementOntology.getInstance());

        this.bSubscriptionResponder = new BSubscriptionResponder(this);
        addBehaviour(this.bSubscriptionResponder);
        addBehaviour(new BMessageGC(this, 300000));
        this.bEventChannel = new BEventChannel(this, this.configuration.getDispatchIntervalMillis(),
                this.configuration.maxTimeWithoutBrokerConnectionMillis, null);
        this.bEventChannel.setMaxFailedDistributionAttempts(this.configuration.maxFailedDistributionAttempts);
        addBehaviour(this.bEventChannel);

        if (Objects.nonNull(this.configuration.topic) && !this.eventTopic.equals(this.configuration.topic)) {
            this.eventTopic = this.configuration.topic;
            this.yup.setServiceDescription(ServiceDescriptionUtils.createEventSourceSD(this.eventTopic));
        }

        int maxRequestsAtOnce = this.configuration.maxSubscribersLimit;
        if (maxRequestsAtOnce > 10) {
            maxRequestsAtOnce /= 3;
        }
        this.bPingResponder = new BPingResponder(this, maxRequestsAtOnce);

        addBehaviour(this.bPingResponder);
        addBehaviour(this.bEventBrokerAnnouncer = new BEventBrokerAnnouncer(this.yup, this.eventTopic));
        if (this.configuration.subscriberAliveCheckIntervalMillis > 0) {
            addBehaviour(new BSubscriberExistsChecker(
                    this, this.configuration.subscriberAliveCheckIntervalMillis, this::unsubscribeAIDForEvents
                    )
            );
        }
    }

    @Override
    public void doActivate() {
        super.doActivate();
        this.bEventBrokerAnnouncer.reset();
        addBehaviour(this.bEventChannel);
        addBehaviour(this.bPingResponder);
        addBehaviour(this.bEventBrokerAnnouncer);
    }

    /**
     * Prepares the agent to stop receiving and dispatching event messages.
     */
    protected void preparePause() {
        this.removeBehaviour(this.bPingResponder);
        if (this.bEventBrokerAnnouncer != null) {
            removeBehaviour(this.bEventBrokerAnnouncer);
            this.bEventBrokerAnnouncer.stop();
            yup.deregister();
        }

        this.bEventChannel.onTick();
        removeBehaviour(this.bEventChannel);

        try {
            this.taskScheduler.awaitTermination(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.agentToIdForBrokerMap.forEach((aid, sd) -> {
            if (sd != null) {
                sd.setAllowConnections(false);
                sd.iterateOverConnections(c -> { if (c != null) c.close(); });
            }
        });

        // Inform registered agents that their events subscription is cancelled
        BSubscriptionResponder.sendUnsubscribedRemotelyMsg(this,
                this.agentToIdForBrokerMap.keySet().toArray(new AID[this.agentToIdForBrokerMap.keySet().size()]));
        this.agentToIdForBrokerMap.clear();
    }

    @Override
    protected void takeDown() {
        this.preparePause();
        if (this.taskScheduler != null) {
            this.taskScheduler.shutdownNow();
        }
    }

    @Override
    public void doSuspend() {
        this.preparePause();
        super.doSuspend();
    }

    /**
     * Universal in the context of the agent way to log not supported operation messages.
     */
    private void logErrorNotSupportedAgentOperation() {
        String op = Thread.currentThread().getStackTrace()[2].getMethodName();
        this.logger.log(Logger.SEVERE, "Not supported operation " + op);
    }

    @Override
    public void doMove(Location destination) {
        logErrorNotSupportedAgentOperation();
    }

    @Override
    public void doClone(Location destination, String newName) {
        logErrorNotSupportedAgentOperation();
    }

    /**
     * Attempts to register {@link FIPANames.ContentLanguage#FIPA_SL} language with {@link SLCodec} codec for
     * a particular agent if a registration has not been performed.
     * @param agent The agent instance for which the registration will be made. Cannot be null.
     * @throws NullPointerException If agent is null.
     */
    public static void registerLangFipaSLIfMissing(Agent agent) {
        Objects.requireNonNull(agent, "Null agent");

        String[] languageNames = agent.getContentManager().getLanguageNames();
        for (String s : languageNames) {
            if (FIPANames.ContentLanguage.FIPA_SL.equals(s)) return;
        }

        agent.getContentManager().registerLanguage(new SLCodec(), FIPANames.ContentLanguage.FIPA_SL);
    }

    /**
     * Checks if a particular Agent ID is subscribed to the current EventBrokerAgent to receive events.
     * @param aid The agent ID to check.
     * @return True if it's subscribed, otherwise false.
     */
    public boolean isAIDSubscribed(AID aid) {
        return (aid != null && this.agentToIdForBrokerMap.containsKey(aid));
    }

    /**
     * Checks if the number of subscribed agents to the current one is within its limitations.
     * @return True if maximum capacity is reached, otherwise false.
     */
    public boolean isSubscriptionCapacityReached() {
        return (this.configuration.maxSubscribersLimit > 0
                && this.agentToIdForBrokerMap.size() >= this.configuration.maxSubscribersLimit);
    }

    /**
     * Checks if the subscription capacity has actually been surpassed.
     * @return True if it has been surpassed otherwise false.
     */
    protected synchronized boolean isPastSubscriptionCapacity() {
        return (this.configuration.maxSubscribersLimit > 0
                && this.agentToIdForBrokerMap.size() > this.configuration.maxSubscribersLimit);
    }

    /**
     * Asynchronously adds agent's ID to internal "list" of agents that are going to be informed for events.
     * @param aid The agent id to subscribe. Cannot be null.
     * @param sp Additional subscription parameters used to uniquely and privately identify the agent.
     * @param timeoutMs The maximum time within the addition to happen. After elapses and the addition is still pending
     *                  the task will be cancelled, terminated and onFailure argument will be executed.
     * @param onSuccess The Runnable to be executed after succeeding. The execution context might be in the current or
     *                  another thread.
     * @param onFailure The Runnable to be executed after failing/timing out. The execution context might be in the
     *                  current or another thread.
     */
    public void subscribeAIDForEvents(AID aid, SubscriptionParameter sp, long timeoutMs, Runnable onSuccess,
                                      Runnable onFailure) {
        // early checks and execution speed optimization
        if (isSubscriptionCapacityReached() || aid == null) { onFailure.run(); return; }

        SubscriberData subscriberData;
        {
            final MutableBoolean isAbsent = new MutableBoolean(false);
            subscriberData = this.agentToIdForBrokerMap.computeIfAbsent(aid, _aid -> {
                isAbsent.setValue(true);
                return new SubscriberData(sp, configuration.eventDeduplicationCapacity);
            });
            if (!isAbsent.booleanValue() && subscriberData != null) {
                onSuccess.run();
                return;
            } else if (subscriberData == null) {
                subscriberData = this.agentToIdForBrokerMap.put(aid,
                        new SubscriberData(sp, configuration.eventDeduplicationCapacity));
            }
        }

        if (this.taskScheduler == null) { // unlikely
            this.taskScheduler = Executors.newScheduledThreadPool(
                    Math.max(6, Math.min(6, Runtime.getRuntime().availableProcessors()))
            );
            ((ScheduledThreadPoolExecutor)this.taskScheduler).setRemoveOnCancelPolicy(true);
        }

        Consumer<BasicConfiguration.Link> linkModifiers =
                (this.configurationRemapper != null ? configurationRemapper.apply(sp) : null);

        List<IEventDispatcher> result = Collections.synchronizedList(new ArrayList<>(configuration.link.size()));
        FutureTask<List<IEventDispatcher>> dispatchersCreatorTask =
                configuration.makeDispatchers(linkModifiers, result, this.taskScheduler);

        final SubscriberData _subscriberData = subscriberData;
        AbstractEventDispatcher.scheduleNow(() -> {
                List<IEventDispatcher> dispatchers = null;
                try {
                    long ts = System.currentTimeMillis();
                    dispatchers = dispatchersCreatorTask.get(timeoutMs, TimeUnit.MILLISECONDS);
                    if (dispatchers != null) {
                        if (dispatchers.isEmpty()) {
                            dispatchers = null;
                        } else {
                            // some message broker systems like Kafka don't block upon instantiation if there's no
                            // connection established. We try to unify such behaviours here.
                            for (Iterator<IEventDispatcher> it = dispatchers.iterator(); it.hasNext(); ) {
                                IEventDispatcher d = it.next();
                                if (!d.isConnected()) {
                                    long slpDiff = System.currentTimeMillis() - ts;
                                    if (slpDiff < timeoutMs) {
                                        Thread.sleep(timeoutMs - slpDiff);
                                    }
                                }
                                if (!d.isConnected())
                                    throw new IllegalStateException("Some dispatchers not created!");
                            }

                            if (isPastSubscriptionCapacity())
                                throw new IllegalStateException("Subscription capacity surpassed");
                        }
                    }
                    if (dispatchers == null) throw new NullPointerException("No dispatchers created");
                    if (!_subscriberData.setConnections(dispatchers)) {
                        throw new UnsupportedOperationException(
                                "Denied setting dispatcher instances for a subscriber AID!" + aid.toString());
                    }
                } catch (Exception e) {
                    dispatchersCreatorTask.cancel(true);
                    if (dispatchers != null) {
                        dispatchers.forEach(c -> { if (c != null) c.close(); });
                        dispatchers.clear();
                    }
                    this.agentToIdForBrokerMap.remove(aid);
                    onFailure.run();
                    return null;
                }

                if (isSubscriptionCapacityReached()) {
                    logger.log(Level.WARNING, "Event broker agent " + getAID() + " reached its subscribers limit -"
                            + " subscribers count " + this.agentToIdForBrokerMap.size());

                    ServiceDescription sd = this.yup.getServiceDescription();

                    ServiceDescriptionUtils.setFirstPropertyNamed(ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS,
                            ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS_NEGATIVE_ANS, sd);

                    CompletableFuture.supplyAsync(() -> {
                        this.yup.deregister(); // might block for too long. that's why we execute it here
                        this.yup.register();
                        return null;
                    }, this.taskScheduler);
                }

                onSuccess.run();
                return null;
            },
                this.taskScheduler
        );
    }

    /**
     * Semi-synchronously adds Agent ID to internal "list" of agents that are going to be informed for events.
     * @param aid The agent id to subscribe. Cannot be null.
     * @param sp Additional subscription parameters used to uniquely and privately identify the agent.
     * @return True if the agent is subscribed, otherwise false.
     * @deprecated Use the asynchronous version of the method instead!
     */
    @Deprecated
    public boolean subscribeAIDForEvents(AID aid, SubscriptionParameter sp) {
        if (isSubscriptionCapacityReached() || aid == null) return false;
        if (isAIDSubscribed(aid)) return true; // execution speed optimization

        final MutableBoolean isAbsent = new MutableBoolean(false);
        final SubscriberData subscriberData = this.agentToIdForBrokerMap.computeIfAbsent(aid, _aid -> {
            isAbsent.setValue(true);
            return new SubscriberData(sp, configuration.eventDeduplicationCapacity);
        });

        if (isAbsent.booleanValue() && subscriberData != null) {
            if (this.taskScheduler == null) { // unlikely
                this.taskScheduler = Executors.newScheduledThreadPool(
                        Math.max(6, Math.min(6, Runtime.getRuntime().availableProcessors()))
                );
                ((ScheduledThreadPoolExecutor)this.taskScheduler).setRemoveOnCancelPolicy(true);
            }

            Consumer<BasicConfiguration.Link> linkModifiers =
                    (this.configurationRemapper != null ? configurationRemapper.apply(sp) : null);

            Supplier<List<IEventDispatcher>> eventDispatchersSupplier =
                    () -> configuration.makeDispatchers(linkModifiers);

            Consumer<ImmutablePair<List<IEventDispatcher>, Boolean>> connectionChecker = (dispatchSupplUndoPair) -> {
                // on failure send to the remote agent "remote unsubscribe" message due to the lack of broker
                // connections
                List<IEventDispatcher> eventDispatchers = dispatchSupplUndoPair.getLeft();
                if (eventDispatchers == null || eventDispatchers.isEmpty()) {
                    BSubscriptionResponder.sendUnsubscribedRemotelyMsg(EventBrokerAgent.this, aid);
                    this.agentToIdForBrokerMap.remove(aid);
                } else if (dispatchSupplUndoPair.getRight().booleanValue()
                        || !subscriberData.setConnections(eventDispatchers)) { // the second arg is unlikely to happen
                    BSubscriptionResponder.sendUnsubscribedRemotelyMsg(EventBrokerAgent.this, aid);
                    this.bSubscriptionResponder.remoteUnsubscribe(aid);
                    eventDispatchers.forEach(c -> { if (c != null) c.close(); });
                    eventDispatchers.clear();
                    this.agentToIdForBrokerMap.remove(aid);
                }
            };

            CompletableFuture<List<IEventDispatcher>> createConnections = AbstractEventDispatcher.scheduleNow(
                    eventDispatchersSupplier::get, this.taskScheduler);
            AbstractEventDispatcher
                    .executeWithin(
                            createConnections,
                            Duration.ofMillis(
                                    configuration.maxTimeWithoutBrokerConnectionMillis > 0
                                            ?
                                            configuration.maxTimeWithoutBrokerConnectionMillis
                                            :
                                            BasicConfiguration.DEFAULT_MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS
                            ),
                            this.taskScheduler
                    )
                    .whenComplete((data, exception) -> {
                            if (exception != null) {
                                createConnections.cancel(true);
                            }
                            connectionChecker.accept(ImmutablePair.of(data, Boolean.valueOf(exception != null)));
                    });
        }

        /* =======================================================================
        final MutableBoolean undoSubscription = new MutableBoolean(false);
        this.agentToIdForBrokerMap.computeIfAbsent(aid, _aid -> {
            SubscriberData subscriberData = new SubscriberData(sp, configuration.eventDeduplicationCapacity);
            Consumer<BasicConfiguration.Link> linkModifiers =
                    (this.configurationRemapper != null ? configurationRemapper.apply(sp) : null);

            undoSubscription.setValue(false);
            Supplier<List<IEventDispatcher>> eventDispatchersSupplier =
                    () -> configuration.makeDispatchers(linkModifiers);

            Consumer<List<IEventDispatcher>> connectionChecker = (eventDispatchers) -> {
                // on failure send to the remote agent "remote unsubscribe" message due to the lack of broker
                // connections
                if (eventDispatchers == null || eventDispatchers.isEmpty()) {
                    BSubscriptionResponder.sendUnsubscribedRemotelyMsg(EventBrokerAgent.this, _aid);
                } else if (undoSubscription.booleanValue()
                        || !subscriberData.setConnections(eventDispatchers)) { // the second arg is unlikely to happen
                    BSubscriptionResponder.sendUnsubscribedRemotelyMsg(EventBrokerAgent.this, _aid);
                    eventDispatchers.forEach(c -> { if (c != null) c.close(); });
                    eventDispatchers.clear();
                }
            };

            if (configuration.maxTimeWithoutBrokerConnectionMillis > 0) {
                if (this.taskScheduler == null) { // unlikely
                    this.taskScheduler = Executors.newScheduledThreadPool(
                            Math.min(2, Runtime.getRuntime().availableProcessors())
                    );
                }
                try {
                    AbstractEventDispatcher
                            .executeWithin(
                                    AbstractEventDispatcher.scheduleNow(
                                            () -> eventDispatchersSupplier.get(),
                                            this.taskScheduler
                                    ),
                                    Duration.ofMillis(configuration.maxTimeWithoutBrokerConnectionMillis),
                                    this.taskScheduler
                            )
                            .whenComplete((data, exception) -> {
                                undoSubscription.setValue(exception == null);
                                connectionChecker.accept(data);
                            });
                }catch (Exception e) {}
            } else {
                CompletableFuture
                        .supplyAsync(eventDispatchersSupplier)
                        .thenAccept(connectionChecker);
            }

            return (subscriberData != null && subscriberData.connections != null
                    && !subscriberData.connections.isEmpty() ? subscriberData : null);
        }); ===================================== */

        if (isSubscriptionCapacityReached()) {
            logger.log(Level.WARNING, "Event broker agent " + getAID()
                    + " reached its subscribers limit - subscribers count " + this.agentToIdForBrokerMap.size());

            ServiceDescription sd = yup.getServiceDescription();

            ServiceDescriptionUtils.setFirstPropertyNamed(ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS,
                    ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS_NEGATIVE_ANS, sd);

            CompletableFuture.supplyAsync(() -> {
                yup.deregister(); // might block for too long. that's why we execute it here
                yup.register();
                return null;
            }, this.taskScheduler);
        }

        /*
        // old==================================================================================
        if (!isAIDSubscribed(aid)) {
            this.agentToIdForBrokerMap.computeIfAbsent(aid, _aid -> {
                SubscriberData subscriberData = new SubscriberData(sp, configuration.eventDeduplicationCapacity);
                Consumer<BasicConfiguration.Link> linkModifiers =
                        (this.configurationRemapper != null ? configurationRemapper.apply(sp) : null);

                Supplier<List<IEventDispatcher>> eventDispatchersSupplier =
                        () -> configuration.makeDispatchers(linkModifiers);

                Consumer<List<IEventDispatcher>> connectionChecker = (eventDispatchers) -> {
                    // on failure send to the remote agent "remote unsubscribe" message due to the lack of broker
                    // connections
                    if (!subscriberData.setConnections(eventDispatchers)) {
                        BSubscriptionResponder.sendUnsubscribedRemotelyMsg(EventBrokerAgent.this, _aid);
                        subscriberData.connections.forEach(c -> { if (c != null) c.close(); });
                        this.agentToIdForBrokerMap.remove(_aid);
                    } else {
                        if (eventDispatchers == null || eventDispatchers.isEmpty()) {
                            BSubscriptionResponder.sendUnsubscribedRemotelyMsg(EventBrokerAgent.this, _aid);
                            this.agentToIdForBrokerMap.remove(_aid);
                        }
                    }
                };

                // TODO FIXME
                subscriberData.markConnectionInitStartTime();

                if (configuration.maxTimeWithoutBrokerConnectionMillis > 0) {
                    if (this.taskScheduler == null) { // unlikely
                        this.taskScheduler = Executors.newScheduledThreadPool(
                                Math.min(2, Runtime.getRuntime().availableProcessors())
                        );
                    }
                    AbstractEventDispatcher
                            .executeWithin(
                                    AbstractEventDispatcher.scheduleNow(
                                            () -> eventDispatchersSupplier.get(),
                                            this.taskScheduler
                                    ),
                                    Duration.ofMillis(configuration.maxTimeWithoutBrokerConnectionMillis),
                                    this.taskScheduler
                            )
                            .whenComplete((data, exception) -> {
                                if (exception == null) {
                                    connectionChecker.accept(data);
                                } else {

                                }
                            });
                } else {
                    CompletableFuture
                            .supplyAsync(eventDispatchersSupplier)
                            .thenAccept(connectionChecker);
                }

                return subscriberData;
            });

            if (isSubscriptionCapacityReached()) {
                logger.log(Level.WARNING, "Event broker agent " + getAID()
                        + " reached its subscribers limit - subscribers count " + this.agentToIdForBrokerMap.size());

                ServiceDescription sd = yup.getServiceDescription();

                ServiceDescriptionUtils.setFirstPropertyNamed(ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS,
                        ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS_NEGATIVE_ANS, sd);

                CompletableFuture.supplyAsync(() -> {
                    yup.deregister(); // might block for too long. that's why we execute it here
                    yup.register();
                    return null;
                });
            }
            */

            /*this.agentToIdForBrokerMap.computeIfAbsent(aid, _aid -> { // a synchronous version of the above
                SubscriberData subscriberData = new SubscriberData(sp, configuration.eventDeduplicationCapacity);
                if (this.configurationRemapper != null) {
                    subscriberData.connections = configuration.makeDispatchers(configurationRemapper.apply(sp));
                } else {
                    subscriberData.connections = configuration.makeDispatchers(null);
                }
                return subscriberData;
            });*/
        /*}*/
        return true;
    }

    /**
     * Removes Agent ID from the internal "list" of agents that are going to be informed for events.
     * @param aid The agent id to be removed.
     */
    public void unsubscribeAIDForEvents(AID aid) {
        if (aid == null) return;
        boolean wasFull = isSubscriptionCapacityReached();
        this.agentToIdForBrokerMap.computeIfPresent(aid, (_aid, sd) -> {
            if (sd.connections != null) {
                sd.setAllowConnections(false);
                sd.iterateOverConnections(c -> { if (c != null) c.close(); });
                sd.connections.clear();
                sd.getLatestDeduplicationKeys().clear();
            }
            return null;
        });
        if (wasFull && !isSubscriptionCapacityReached()) {
            ServiceDescription sd = yup.getServiceDescription();

            ServiceDescriptionUtils.setFirstPropertyNamed(ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS,
                    ServiceDescriptionUtils.ACCEPT_NEW_SUBSCRIBERS_AFFIRMATIVE_ANS, sd);

            CompletableFuture.supplyAsync(() -> {
                yup.deregister(); // might block for too long. that's why we execute it here
                yup.register();
                return null;
            }, this.taskScheduler);
        }
    }

    /**
     * Removes Agent ID from the internal "list" of agents that are going to be informed for events and also informs
     * the Agent ID that has been unsubcribed "remotely".
     * @param aid The agent id to be removed.
     */
    public void unsubscribeAIDForEventsAndInform(AID aid) {
        this.bSubscriptionResponder.remoteUnsubscribe(aid);
    }

    /**
     * Returns copy set of Agent IDs subscribed for events.
     * @return Immutable collection of AIDs.
     */
    public Collection<AID> getSubscribedAIDs() {
        return Collections.unmodifiableCollection(this.agentToIdForBrokerMap.keySet());
    }

    /**
     * Returns subscriber data (effectively broker instances) associations in the current agent instance.
     * @param aid The agent identifier for which to get the subscriber data (if any).
     * @return A SubscriberData instance with the current associations or null.
     */
    public SubscriberData getSubscriberDataForAid(AID aid) {
        return agentToIdForBrokerMap.get(aid);
    }

    /**
     * Returns the current number of subscribed agents.
     * @return The number of subscribed agents as integer.
     */
    public int subscribersCount() {
        return this.agentToIdForBrokerMap.size();
    }
}
