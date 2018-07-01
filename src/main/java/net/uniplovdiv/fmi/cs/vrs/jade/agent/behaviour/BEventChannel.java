package net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour;

import jade.content.Concept;
import jade.content.ContentElement;
import jade.content.onto.basic.Result;
import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.TickerBehaviour;
import jade.domain.FIPANames;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import jade.util.Logger;
import net.uniplovdiv.fmi.cs.vrs.event.IEvent;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.IEventDispatcher;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.encapsulation.DataEncodingMechanism;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.encapsulation.DataPacket;
import net.uniplovdiv.fmi.cs.vrs.event.parameters.ParametersContainer;
import net.uniplovdiv.fmi.cs.vrs.event.serializers.IEventSerializer;
import net.uniplovdiv.fmi.cs.vrs.event.serializers.JavaEventSerializer;
import net.uniplovdiv.fmi.cs.vrs.event.serializers.engine.Base32Encoder;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.EventBrokerAgent;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.configuration.BasicConfiguration;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.AExchangeEvent;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.EventData;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.EventEngineOntology;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Behaviour used to send and receive events to/from EventBroker agents and ordinary agents interested in communicating
 * using Event Engine's events.
 */
public class BEventChannel extends TickerBehaviour {
    private static final long serialVersionUID = 8229887551133593731L;

    /**
     * The name of the protocol used for events exchange.
     */
    public static final String EVENT_EXCHANGE_PROTOCOL_NAME = "event-engine-channel-event-exchange";

    /**
     * Used to be embedded into event's instances for deduplication in case of sending them at once through more than 1
     * broker environment and receiving them again on the other end.
     */
    public static final String EVENT_DEDUPLICATE_KEY = "__deduplicate_key__";

    private final Logger logger;

    private final static MessageTemplate INCOMING_EVENT_MSG_TEMPLATE_BROKER = MessageTemplate.and(
            MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
            MessageTemplate.and(
                    MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                    MessageTemplate.and(
                            MessageTemplate.MatchProtocol(EVENT_EXCHANGE_PROTOCOL_NAME),
                            MessageTemplate.or(
                                    MessageTemplate.MatchPerformative(ACLMessage.PROXY),
                                    MessageTemplate.or(
                                            MessageTemplate.MatchPerformative(ACLMessage.FAILURE),
                                            MessageTemplate.MatchPerformative(ACLMessage.NOT_UNDERSTOOD)
                                    )
                            )
                    )
            )
    );

    private final static MessageTemplate INCOMING_EVENT_MSG_TEMPLATE_CLIENT_RECV_CONFIRMATION = MessageTemplate.and(
            MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
            MessageTemplate.and(
                    MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                    MessageTemplate.and(
                            MessageTemplate.MatchProtocol(EVENT_EXCHANGE_PROTOCOL_NAME),
                            MessageTemplate.or(
                                    MessageTemplate.MatchPerformative(ACLMessage.CONFIRM),
                                    MessageTemplate.or(
                                            MessageTemplate.MatchPerformative(ACLMessage.FAILURE),
                                            MessageTemplate.or(
                                                    MessageTemplate.MatchPerformative(ACLMessage.NOT_UNDERSTOOD),
                                                    MessageTemplate.MatchPerformative(ACLMessage.REFUSE)
                                            )
                                    )
                            )
                    )
            )
    );

    private final static MessageTemplate INCOMING_EVENT_MSG_TEMPLATE_CLIENT_RECV_EVENT = MessageTemplate.and(
            MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
            MessageTemplate.and(
                    MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                    MessageTemplate.and(
                            MessageTemplate.MatchProtocol(EVENT_EXCHANGE_PROTOCOL_NAME),
                            MessageTemplate.or(
                                    MessageTemplate.MatchPerformative(ACLMessage.INFORM),
                                    MessageTemplate.or(
                                            MessageTemplate.MatchPerformative(ACLMessage.FAILURE),
                                            MessageTemplate.MatchPerformative(ACLMessage.NOT_UNDERSTOOD)
                                    )
                            )
                    )
            )
    );

    /*private final static MessageTemplate INCOMING_EVENT_MSG_TEMPLATE = MessageTemplate.and(
            MessageTemplate.MatchLanguage(FIPANames.ContentLanguage.FIPA_SL),
            MessageTemplate.and(
                    MessageTemplate.MatchOntology(EventEngineOntology.NAME),
                    MessageTemplate.and(
                            MessageTemplate.or(
                                    MessageTemplate.MatchPerformative(ACLMessage.INFORM),
                                    MessageTemplate.or(
                                            MessageTemplate.or(
                                                    MessageTemplate.MatchPerformative(ACLMessage.PROXY),
                                                    MessageTemplate.MatchPerformative(ACLMessage.FAILURE)
                                            ),
                                            MessageTemplate.MatchPerformative(ACLMessage.NOT_UNDERSTOOD)
                                    )
                            ),
                            MessageTemplate.MatchProtocol(EVENT_EXCHANGE_PROTOCOL_NAME)
                    )
            )
    );*/

    /*private static class IEventBiConsumerPair { // TODO use in future
        public IEvent event;
        public BiConsumer<Boolean, IEvent> callback;

        public IEventBiConsumerPair() { }

        public IEventBiConsumerPair(IEvent event, BiConsumer<Boolean, IEvent> callback) {
            this.event = event;
            this.callback = callback;
        }

        @Override
        public int hashCode() {
            return (event != null ? event.hashCode() : 0);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj instanceof IEventBiConsumerPair) {
                IEventBiConsumerPair iebcp = (IEventBiConsumerPair) obj;
                return (Objects.equals(this.event, iebcp.event) && this.callback == iebcp.callback;
            }
            return false;
        }
    }*/

    private short failedDistributionAttempts = 0;
    private short maxFailedDistributionAttempts = 5;
    private int msgCountToDiscard = 0;

    private EventBrokerAgent myEventBrokerAgent = null;
    private BEventBrokerSubscriber eventSubscrbBehaviour = null;
    private Consumer<IEvent> onReceiveEvent;

    private Deque<IEvent> qSend = new ConcurrentLinkedDeque<>();
    private Deque<IEvent> qRecv = new ConcurrentLinkedDeque<>();

    private IEventSerializer eventSerializerInst = null;
    private Base32Encoder base32EncoderInst = null;

    private long maxTimeWithoutBrokerConnectionMillis =
            BasicConfiguration.DEFAULT_MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS;
    private ConcurrentMap<AID, Long> lastConnectionlessPeriodsMap = null;

    /**
     * Constructor to be used by ordinary agents interested in sending or receiving events.
     * @param agent The agent to associate this behaviour with.
     * @param dispatchIntervalMillis The interval on which the actual event dispatching will occur.
     * @param eventSubscrbBehaviour An instance of the {@link BEventBrokerSubscriber} behaviour.
     * @param onReceiveEvent A callback instance executed when new events are received. If null you must manually poll
     *                       for newly received events by using {@link BEventChannel#receive()}.
     * @throws IllegalArgumentException If deduplicationCapacity is not greater than 0.
     */
    public BEventChannel(Agent agent, long dispatchIntervalMillis, BEventBrokerSubscriber eventSubscrbBehaviour,
                         Consumer<IEvent> onReceiveEvent) {
        super(agent, dispatchIntervalMillis);
        this.logger = Logger.getJADELogger(agent.getClass().getName());
        this.eventSubscrbBehaviour = eventSubscrbBehaviour;
        this.onReceiveEvent = onReceiveEvent;
    }

    /**
     * Constructor to be used by Event Broker agents interested in distribution, sending and receiving events.
     * @param agent The Event Broker agent the behaviour to be associated with.
     * @param dispatchIntervalMillis The interval on which the actual event dispatching will occur.
     * @param maxTimeWithoutBrokerConnectionMillis The maximum amount of time for the dispatcher agent to tolerate lack
     *                                             of connection to the broker. See {@link
     * net.uniplovdiv.fmi.cs.vrs.jade.agent.configuration.BasicConfiguration#maxTimeWithoutBrokerConnectionMillis}.
     * @param onReceiveEvent  A callback instance executed when new events are received. If null you must manually poll
     *                        for newly received events by using {@link BEventChannel#receive()}.
     */
    public BEventChannel(EventBrokerAgent agent, long dispatchIntervalMillis, long maxTimeWithoutBrokerConnectionMillis,
                         Consumer<IEvent> onReceiveEvent) {
        super(agent, dispatchIntervalMillis);
        this.maxTimeWithoutBrokerConnectionMillis = (maxTimeWithoutBrokerConnectionMillis > 0
                ?
                maxTimeWithoutBrokerConnectionMillis
                :
                BasicConfiguration.DEFAULT_MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS
        );
        this.logger = Logger.getJADELogger(agent.getClass().getName());
        this.myEventBrokerAgent = agent;
        this.onReceiveEvent = onReceiveEvent;
        this.lastConnectionlessPeriodsMap = new ConcurrentHashMap<>();
    }

    /**
     * Returns the amount of retries to do per a specific batch of event data when attempting to send it.
     * @return A value greater than 0 indicates the amount attempts performed per failed batch of events. Alternative
     *         values designate the lack of any action.
     */
    public short getMaxFailedDistributionAttempts() {
        return this.maxFailedDistributionAttempts;
    }

    /**
     * Changes the amount of retries to do per a specific batch of event data when attempting to send it. If all
     * attempts fail the batch will be discarded. Setting negative or 0 value will result no retries to be executed.
     * @param maxFailedDistributionAttempts The amount of attempts.
     */
    public void setMaxFailedDistributionAttempts(short maxFailedDistributionAttempts) {
        this.maxFailedDistributionAttempts = maxFailedDistributionAttempts;
    }

    /**
     * Lazy one-time instantiator of {@link IEventSerializer}. By default is using {@link JavaEventSerializer}.
     * @return A new event serializer instance if the method is called for the first time, otherwise the previously
     *         created instance.
     */
    protected IEventSerializer makeEventSerializerInstance() {
        return (this.eventSerializerInst != null ?
                this.eventSerializerInst :
                (this.eventSerializerInst = new JavaEventSerializer()));
    }

    /**
     * Lazy one-time instantiator of {@link Base32Encoder}.
     * @return A new encoder instance if the method is called for the first time, otherwise the previously created
     *         instance.
     */
    protected Base32Encoder makeBase32EncoderInstance() {
        return (this.base32EncoderInst != null ?
                this.base32EncoderInst :
                (this.base32EncoderInst = new Base32Encoder()));
    }

    /**
     * Schedules event for sending. The event might be further modified during the sending process, thus it will be
     * copied. Providing event that does not implement a constructor for copy might produce undefined behaviour in your
     * program.
     * @param event The event to be sent. Cannot be null.
     */
    public void send(IEvent event) {
        if (event != null) {
            Constructor<? extends IEvent> ctor;
            try {
                ctor = event.getClass().getConstructor(event.getClass());
                event = ctor.newInstance(event);
            } catch (Throwable ex) {
                try {
                    ctor = event.getClass().getDeclaredConstructor(event.getClass());
                    ctor.setAccessible(true);
                    event = ctor.newInstance(event);
                } catch (Throwable ex2) {
                    logger.log(Level.WARNING, "Cannot clone via copy ctor event " + event.toString(), ex2);
                }
            }
            this.qSend.add(event);
        }
    }

    /**
     * Clears the internal event sending queue, returning the events that have been removed from it.
     * @return A nonempty list of removed events or null.
     */
    public List<IEvent> clearSendQueue() {
        if (this.qSend.isEmpty()) return null;
        List<IEvent> result = new ArrayList<>(this.qSend.size());
        for (IEvent element; (element = this.qSend.pop()) != null; result.add(element));
        return (!result.isEmpty() ? result : null);
    }

    /**
     * Receives an event. This method will return events only if no callback receiving method has been specified.
     * @return An IEvent instance or null.
     */
    public IEvent receive() {
        return this.qRecv.poll();
    }

    /**
     * Checks if the current instance has been configured by {@link EventBrokerAgent} and thus works in such mode or
     * by an ordinary agent.
     * @return True if it has been configured by {@link EventBrokerAgent} otherwise false.
     */
    private boolean isUsedByEventBroker() {
        return this.myEventBrokerAgent != null;
    }

    @Override
    public void onTick() {
        if (this.isUsedByEventBroker()) {

            // Check if there are real message broker connections that are down for too long and remotely unsubscribe
            // the agents associated with them

            this.myEventBrokerAgent.getSubscribedAIDs().forEach(aid -> {
                EventBrokerAgent.SubscriberData sd = myEventBrokerAgent.getSubscriberDataForAid(aid);
                if (sd == null) {
                    this.lastConnectionlessPeriodsMap.remove(aid);
                    return;
                }

                MutableBoolean isConnectionProblem = new MutableBoolean(false);
                sd.iterateOverConnections(iEventDispatcher -> {
                    if (!isConnectionProblem.booleanValue() && !iEventDispatcher.isConnected()) {
                        isConnectionProblem.setValue(true);
                    }
                });

                if (isConnectionProblem.booleanValue()) {
                    Long noConnectionTs = this.lastConnectionlessPeriodsMap.get(aid);
                    if (noConnectionTs == null) {
                        this.lastConnectionlessPeriodsMap.put(aid, Long.valueOf(System.currentTimeMillis()));
                    } else {
                        if (Math.abs(System.currentTimeMillis() - noConnectionTs.longValue()) >
                                this.maxTimeWithoutBrokerConnectionMillis) {
                            // mark the agents which will be unsubscribed remotely
                            this.lastConnectionlessPeriodsMap.remove(aid);
                            this.myEventBrokerAgent.unsubscribeAIDForEventsAndInform(aid);
                            logger.warning("Unsubscribing " + aid
                                    + " due to the lack of full broker connectivity!");
                        }
                    }
                }
            });

            // EventBrokerAgent - send events "to subscribed agents" via Event Engine's EventDispatcher:
            for (IEvent event = this.qSend.poll(); event != null; event = this.qSend.poll()) {
                for (AID aid : this.myEventBrokerAgent.getSubscribedAIDs()) {
                    EventBrokerAgent.SubscriberData sd = this.myEventBrokerAgent.getSubscriberDataForAid(aid);
                    if (sd != null) {
                        final IEvent _event = event;
                        {
                            List<IEventDispatcher> connections = sd.getConnections();
                            if (connections != null && connections.size() > 1) embedEventDeduplicationKey(_event);
                        }

                        sd.iterateOverConnections(ied -> {
                            if (ied != null) ied.send(_event, null);
                        });
                    }
                }
            }

            // EventBrokerAgent - receives events from Event Engine's EventDispatcher for each of the current
            // subscribers and send the events to them as a message
            this.myEventBrokerAgent.getSubscribedAIDs().parallelStream().forEach(aid -> {
                EventBrokerAgent.SubscriberData sd = myEventBrokerAgent.getSubscriberDataForAid(aid);
                if (sd == null) return;

                final List<List<IEvent>> allEvents = new ArrayList<>();

                sd.iterateOverConnections(ied -> {
                    if (ied != null) {
                        List<IEvent> events = ied.receive(300);
                        if (events != null && !events.isEmpty()) allEvents.add(events);
                    }
                });

                Collection<String> latestDeduplicationKeys = sd.getLatestDeduplicationKeys();

                List<byte[]> data = allEvents.stream().flatMap(Collection::stream)
                        .filter((theEvent) -> {
                            ParametersContainer pc = theEvent.getDynamicParameters();
                            if (pc != null) {
                                Object edk = pc.get(EVENT_DEDUPLICATE_KEY);
                                if (edk != null) {
                                    String sEdk = edk.toString();
                                    return (sEdk.isEmpty() || !latestDeduplicationKeys.add(sEdk));
                                }
                            }
                            return true;
                        })
                        .map(event -> {
                            byte[] _data = null;
                            try {
                                DataPacket dp = encodeEventToB32DataPacket(event);
                                if (dp != null) _data = dp.toBytes();
                            } catch (Exception e) {
                                logger.log(Logger.SEVERE, e.getMessage(), e);
                            }
                            return _data;
                        })
                        .filter(binEvent -> binEvent != null && binEvent.length > 0)
                        .collect(Collectors.toList());

                if (!data.isEmpty()) {  // there's event data. Sent it to the receiver agent...
                    EventData ed = new EventData();
                    ed.setData(data);
                    ACLMessage newEvent = this.prepareEventDataMsg(aid);
                    try {
                        myEventBrokerAgent.getContentManager()
                                .fillContent(newEvent, new Result(new AExchangeEvent(), ed));
                        myEventBrokerAgent.send(newEvent);
                    } catch (Exception e) {
                        logger.log(Logger.SEVERE, e.getMessage(), e);
                    }
                }
            });

            // EventBrokerAgent - receive events from normal (subscribed) agents as ordinary messages and proxy them
            // to the broker (real system)
            EventData ed = this.receiveEventMsg();
            if (ed != null && ed.getSource() != null && ed.getData() != null) {
                EventBrokerAgent.SubscriberData sd = this.myEventBrokerAgent.getSubscriberDataForAid(ed.getSource());
                if (sd != null) {
                    final boolean addDeduplicate;
                    {
                        List<IEventDispatcher> connections = sd.getConnections();
                        addDeduplicate = (connections != null && connections.size() > 1);
                    }

                    final List<IEvent> eventsToSend = new ArrayList<>();
                    for (byte[] data : ed.getData()) {
                        if (data != null && data.length > 0) {
                            IEvent event = this.decodeRawB32DataPacketToEvent(data);
                            if (event != null) {
                                eventsToSend.add(event);
                                if (addDeduplicate) embedEventDeduplicationKey(event);
                            }
                        }
                    }

                    sd.iterateOverConnections(ied -> {
                        if (ied == null) return;
                        eventsToSend.forEach(ev -> ied.send(ev, null));
                        /*for (byte[] data : ed.getData()) {
                            if (data != null && data.length > 0) {
                                IEvent event = this.decodeRawB32DataPacketToEvent(data);
                                if (event != null) {
                                    if (addDeduplicate) embedEventDeduplicationKey(event);
                                    ied.send(event, null);
                                }
                            }
                        }*/
                    });
                }
            }

        } else { // is used by ordinary agent to send/receive events
            if (!this.qSend.isEmpty()) {

                // check if there's a broker and if the connection is mature enough to be used
                AID dest = this.eventSubscrbBehaviour.getChosenEventSourceAgent();
                if (dest != null
                        && (System.currentTimeMillis() - this.eventSubscrbBehaviour.getLastSubscriptionTimestamp() >
                            this.eventSubscrbBehaviour.getPingTimeoutMs())) {

                    List<byte[]> rawData = new ArrayList<>(this.qSend.size());
                    for (IEvent event = this.qSend.poll(); event != null; event = this.qSend.poll()) {
                        DataPacket dp = this.encodeEventToB32DataPacket(event);
                        if (dp != null) {
                            rawData.add(dp.toBytes());
                        }
                    }

                    // test once again whether there's data and destination
                    if (!rawData.isEmpty() && (dest = this.eventSubscrbBehaviour.getChosenEventSourceAgent()) != null) {
                        EventData ed = new EventData();
                        ed.setData(rawData);

                        ACLMessage msg = this.prepareEventDataMsg(dest);
                        String replyPassword = Integer.toString(ed.hashCode());
                        msg.setReplyWith(replyPassword);
                        try {
                            Agent agent = getAgent();
                            agent.getContentManager().fillContent(msg, new Result(new AExchangeEvent(), ed));
                            agent.send(msg);
                            MessageTemplate mt = MessageTemplate.and(
                                    INCOMING_EVENT_MSG_TEMPLATE_CLIENT_RECV_CONFIRMATION,
                                    MessageTemplate.MatchInReplyTo(replyPassword)
                            );

                            ACLMessage answer = agent.blockingReceive(mt, 10000);
                            if (answer != null && answer.getPerformative() != ACLMessage.CONFIRM) {
                                // Message not received for some reason. Return the events in queue for later attempt.

                                // on REFUSE the sender might actually be subscribed, but the remote site not to have
                                // prepared the connection links yet. However refuse is quite improbable with the async
                                // version of the subscriber creator logic on the agent broker's side.
                                boolean isRefusal = (answer.getPerformative() == ACLMessage.REFUSE);

                                if (this.failedDistributionAttempts++ == 0) {
                                    this.msgCountToDiscard = rawData.size();
                                } else if (isRefusal &&
                                        this.failedDistributionAttempts >= this.maxFailedDistributionAttempts * 2 - 2) {
                                    // too many refuses, we'd better search another broker to subscribe to
                                    this.eventSubscrbBehaviour.resubscribe(false);
                                }

                                // restore the events or discard some if too many attempts happened
                                // on refusal we're twice more generous for keeping the pending events
                                for (int i = 0; i < rawData.size(); ++i) {
                                    if (this.failedDistributionAttempts >=
                                            (this.maxFailedDistributionAttempts * (isRefusal ? 2 : 1))
                                            && this.msgCountToDiscard > 0) {
                                        logger.log(Logger.SEVERE,
                                                "Maximum attempts to send event data packet reached. Discarded the"
                                                + " oldest " + this.msgCountToDiscard + " events"
                                        );
                                        i = this.msgCountToDiscard;
                                        this.msgCountToDiscard = this.failedDistributionAttempts = 0;
                                        continue;
                                    }

                                    IEvent event = this.decodeRawB32DataPacketToEvent(rawData.get(i));
                                    if (event != null) {
                                        this.qSend.addFirst(event);
                                    } else { // An error when restoring the event occurred. Discard that one.
                                        logger.log(Level.SEVERE,
                                                "Discarded events failed to be restored for later sending.");
                                        if (this.msgCountToDiscard > 0) {
                                            --this.msgCountToDiscard;
                                        }
                                    }
                                }
                            } else {
                                this.msgCountToDiscard = this.failedDistributionAttempts = 0;
                                if (answer != null) {
                                    if (answer.getPerformative() != ACLMessage.CONFIRM) {
                                        logger.log(Level.SEVERE,
                                                "The receiver couldn't understand/receive the sent event!");
                                    }
                                } else {
                                    logger.log(Level.WARNING, "The receiver hasn't responded to the sent event yet!");
                                }
                            }
                        } catch (Exception e) {
                            logger.log(Logger.SEVERE, e.getMessage(), e);
                        }
                    }
                }
            }

            // receive any pending events currently as messages in the agent's queue

            EventData eventData = this.receiveEventMsg();
            if (eventData != null) {
                List<byte[]> data = eventData.getData();
                if (data != null && !data.isEmpty()) {
                    data.stream().map(this::decodeRawB32DataPacketToEvent)
                            .filter(Objects::nonNull)
                            .filter(distinctByKey((theEvent) -> { // duplicates are not expected, but just in case
                                ParametersContainer pc = theEvent.getDynamicParameters();
                                return (pc != null ? pc.get(EVENT_DEDUPLICATE_KEY) : null);
                            }))
                            .forEach(event -> {
                                if (onReceiveEvent != null) {
                                    onReceiveEvent.accept(event);
                                } else {
                                    this.qRecv.add(event);
                                }
                            });
                }
            }
        }
    }

    /**
     * Encodes event to nested base32-java object serialization {@link DataPacket}.
     * @param event The event to be placed inside the packet.
     * @return An instance of {@link DataPacket} or null in case of problems.
     */
    protected DataPacket encodeEventToB32DataPacket(IEvent event) {
        try {
            DataPacket dp = new DataPacket(DataEncodingMechanism.JAVA, null,
                    this.makeEventSerializerInstance().serialize(event));
            dp = new DataPacket(DataEncodingMechanism.BASE32, DataPacket.Version.NESTED, dp.getEncoding(),
                    this.makeBase32EncoderInstance().encode(dp.toBytes()));
            return dp;
        } catch (Exception e) {
            logger.log(Logger.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Decodes raw data representing nested base32-java object serialization {@link DataPacket} to {@link IEvent}.
     * @param rawData The data to be decoded.
     * @return An instance of {@link IEvent} or null in case of problems.
     */
    protected IEvent decodeRawB32DataPacketToEvent(byte[] rawData) {
        try {
            DataPacket dp = new DataPacket(this.makeBase32EncoderInstance().decode(
                    new DataPacket(rawData).getPayload()));
            return this.makeEventSerializerInstance().deserialize(dp.getPayload());
        } catch (Exception e) {
            logger.log(Logger.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Adds a dynamic parameter deduplication key used to remove duplicating events originating from different broker
     * systems.
     * @param event The event to work with. Cannot be null.
     */
    protected void embedEventDeduplicationKey(IEvent event) {
        ParametersContainer dynamicParameters = event.getDynamicParameters();
        if (dynamicParameters == null) {
            dynamicParameters = new ParametersContainer();
            event.setDynamicParameters(dynamicParameters);
        } else if (dynamicParameters.containsKey(EVENT_DEDUPLICATE_KEY)) {
            return;
        }

        String val = UUID.randomUUID().toString() + Long.toString(event.getTimestampMs());
        if (event.getTimestampMs() != event.getValidFromTimestampMs()) {
            val += Long.toString(event.getValidFromTimestampMs());
            if (event.getValidFromTimestampMs() != event.getValidThroughTimestampMs()) {
                val += Long.toString(event.getValidThroughTimestampMs());
            }
        } else if (event.getTimestampMs() != event.getValidThroughTimestampMs()) {
            val += Long.toString(event.getValidThroughTimestampMs());
        }

        dynamicParameters.put(EVENT_DEDUPLICATE_KEY, val);
    }

    /**
     * Used in stream operations for complex distinct combined with filtering, usually to deduplicate event steam.
     * The deduplication is executed when the key extractor function returns non null value (i.e null duplications
     * are allowed).
     * @param keyExtractor The extractor by key function.
     * @param <T> The data type to work with.
     * @return The value that has been previously seen or a null if it's a first unique value ever.
     */
    private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> {
            Object k = keyExtractor.apply(t);
            return (k == null || seen.add(k));
        };
    }

    /**
     * Creates a new ACL message for the other agents with event data.
     * @param destination The agent identifier of the receiver.
     * @return An initialized with basic parameters instance of ACLMessage.
     */
    protected ACLMessage prepareEventDataMsg(AID destination) {
        ACLMessage msg = new ACLMessage(this.isUsedByEventBroker() ? ACLMessage.INFORM : ACLMessage.PROXY);
        msg.addReceiver(destination);
        msg.setLanguage(FIPANames.ContentLanguage.FIPA_SL);
        msg.setOntology(EventEngineOntology.NAME);
        msg.setProtocol(EVENT_EXCHANGE_PROTOCOL_NAME);
        return msg;
    }

    /**
     * Polls agent's message queue for ACL messages that contain {@link EventData} and takes out the first one.
     * In addition tries sends a confirmation message to the sender (agent) of the event.
     * @return An {@link EventData} instance or null if there are none.
     */
    protected EventData receiveEventMsg() {
        Agent agent = getAgent();

        if (this.isUsedByEventBroker()) {
            ACLMessage msg = agent.receive(INCOMING_EVENT_MSG_TEMPLATE_BROKER);
            if (msg == null) return null;

            ACLMessage resp = null;
            try {
                resp = msg.createReply();
                ContentElement ce = agent.getContentManager().extractContent(msg);
                if (ce instanceof Result) {
                    Concept action = ((Result) ce).getAction();
                    Object value = ((Result) ce).getValue();
                    if (action instanceof AExchangeEvent && value instanceof EventData) {
                        AID senderAid = msg.getSender();
                        if (senderAid != null && myEventBrokerAgent.getSubscriberDataForAid(senderAid) != null) {
                            // confirm the message is received
                            resp.setPerformative(ACLMessage.CONFIRM);
                            resp.setReplyWith(null);
                            agent.send(resp);

                            List<byte[]> data = ((EventData) value).getData();
                            if (data != null && !data.isEmpty()) {
                                ((EventData) value).setSource(senderAid);
                                return (EventData) value;
                            }
                        } else {
                            // confirm the message is received, but since the agent is not subscriber we deny it
                            resp.setPerformative(ACLMessage.REFUSE);
                            resp.setReplyWith(null);
                            agent.send(resp);
                        }
                    } else {
                        // confirm the message is received, but not supported
                        resp.setPerformative(ACLMessage.NOT_UNDERSTOOD);
                        resp.setReplyWith(null);
                        agent.send(resp);
                    }
                } else {
                    if (msg.getPerformative() != ACLMessage.NOT_UNDERSTOOD) {
                        // confirm the message is received, but not supported
                        resp.setPerformative(ACLMessage.NOT_UNDERSTOOD);
                        resp.setReplyWith(null);
                        agent.send(resp);
                    }
                }
            } catch (Exception e) {
                logger.log(Logger.SEVERE, e.getMessage(), e);
                if (resp != null) {
                    // confirm the message is received, but not supported
                    resp.setPerformative(ACLMessage.NOT_UNDERSTOOD);
                    resp.setReplyWith(null);
                    agent.send(resp);
                }
            }
        } else {
            // user by ordinary agent to receive new events from the broker
            ACLMessage msg = agent.receive(INCOMING_EVENT_MSG_TEMPLATE_CLIENT_RECV_EVENT);
            if (msg == null) return null;

            try {
                ContentElement ce = null;
                if (msg.getPerformative() == ACLMessage.INFORM) {
                    ce = agent.getContentManager().extractContent(msg);
                }
                if (ce instanceof Result) {
                    Concept action = ((Result) ce).getAction();
                    Object value = ((Result) ce).getValue();
                    if (action instanceof AExchangeEvent && value instanceof EventData) {
                        List<byte[]> data = ((EventData) value).getData();
                        if (data != null && !data.isEmpty()) {
                            ((EventData) value).setSource(msg.getSender());
                            return (EventData) value;
                        }
                    }
                } else {
                    logger.log(Logger.SEVERE, "Received unproductive message " + msg.toString());
                }
            } catch (Exception e) {
                logger.log(Logger.SEVERE, e.getMessage(), e);
            }
        }
        return null;
    }
}
