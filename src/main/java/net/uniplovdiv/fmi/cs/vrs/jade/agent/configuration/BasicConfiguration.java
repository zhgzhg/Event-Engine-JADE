package net.uniplovdiv.fmi.cs.vrs.jade.agent.configuration;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

import jade.util.Logger;
import net.uniplovdiv.fmi.cs.vrs.event.IEvent;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.IEventDispatcher;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.AbstractBrokerConfigFactory;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.AbstractEventDispatcher;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.DispatchingType;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.activemq.ConfigurationFactoryActiveMQ;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.activemq.EventDispatcherActiveMQ;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.kafka.ConfigurationFactoryKafka;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.kafka.EventDispatcherKafka;
import net.uniplovdiv.fmi.cs.vrs.event.dispatchers.encapsulation.DataEncodingMechanism;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Basic configuration class containing settings required to instantiate internal components of the agent like instances
 * of Broker systems and their specific setting parameters.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BasicConfiguration {
    /**
     * Used encoding mechanism when serializing events. See {@link DataEncodingMechanism}.
     */
    public DataEncodingMechanism dataEncodingMechanism;

    /**
     * Dispatching mechanism/operation mode. See {@link DispatchingType}.
     */
    public DispatchingType dispatchingType;

    /**
     * The topic through with events will be sent or received from. See {@link IEvent#getCategory()}.
     */
    public String topic;

    /**
     * The amount of retries to do per a specific batch of event data when attempting to send it. If all attempts fail
     * the batch will be discarded. Setting the value to 0 or negative number will result no retries to be executed.
     * By default it's set to 5.
     */
    public short maxFailedDistributionAttempts = 5;

    /**
     * The size of the memory used to store data for all links (see below) per subscriber, used for deduplication of the
     * same events arriving from multiple broker systems. The minimum recommended value is the total sum of
     * "latestEventsRememberCapacity" for all links, multiplied by the number of links.
     */
     public int eventDeduplicationCapacity = 40;

    /**
     * How often in milliseconds the agent to check for events pending for dispatching. Must be a number greater than 0.
     * By default it's set to 300.
     */
    public long dispatchIntervalMillis = 300;

    /**
     * The maximum amount of subscribers that the agent can work with. Exceeding the number will cause subscription
     * denial to be sent to the subscriber candidate. If set to 0 or a negative number no limitation will be set.
     * By default it's set to 100.
     */
    public int maxSubscribersLimit = 100;

    /**
     * How often in milliseconds the agent to check its subscribers if they are alive. If the number is less than 1
     * then no checks will be executed (not recommended).
     */
    public long subscriberAliveCheckIntervalMillis = 50000;

    public static final long DEFAULT_MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS = 35000;

    /**
     * The maximum amount of time in milliseconds for the dispatcher agent to tolerate lack of connection to the broker.
     * After elapses any subscribers will be informed and unsubscribed from the broker. If more that one broker system
     * is used then it is expected for all connections to be present. Failing to accomplish this will trigger the time
     * countdown and execution of the aforementioned actions. Value of 0 or less will force selecting the default value
     * of {@link #DEFAULT_MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS}.
     */
    public long maxTimeWithoutBrokerConnectionMillis = DEFAULT_MAX_TIME_WITHOUT_BROKER_CONNECTION_MILLIS;

    /**
     * Settings with the "link" to a particular broker system.
     */
    public static class Link {
        private Class<? extends AbstractEventDispatcher> abstractEventDispatcher;
        private Properties configurationData;

        /**
         * See {@link AbstractEventDispatcher#AbstractEventDispatcher(AbstractBrokerConfigFactory, int, boolean)}.
         */
        public int latestEventsRememberCapacity;

        /**
         * See {@link AbstractEventDispatcher#AbstractEventDispatcher(AbstractBrokerConfigFactory, int, boolean)}.
         */
        public boolean doNotReceiveEventsFromSameSource;

        /**
         * See {@link
         * net.uniplovdiv.fmi.cs.vrs.event.dispatchers.brokers.activemq.EventDispatcherActiveMQ#EventDispatcherActiveMQ(
         * ConfigurationFactoryActiveMQ, int, boolean, boolean, String[])}
         */
        public boolean beRetroactive;

        /**
         * Constructor.
         */
        public Link() { }

        /**
         * Copy constructor.
         * @param link The existing instance whose data to copy.
         */
        public Link(Link link) {
            if (link != null) {
                this.abstractEventDispatcher = link.abstractEventDispatcher;
                if (link.configurationData != null) {
                    this.configurationData = new Properties();
                    link.configurationData.forEach(this.configurationData::put);
                }
                this.latestEventsRememberCapacity = link.latestEventsRememberCapacity;
                this.doNotReceiveEventsFromSameSource = link.doNotReceiveEventsFromSameSource;
                this.beRetroactive = link.beRetroactive;
            }
        }

        /**
         * Sets the class of the utility used to provide connection to a particular broker system.
         * @param className The canonical class name of the target.
         * @throws ClassNotFoundException If the specified class cannot be found.
         */
        @SuppressWarnings("unchecked")
        @JsonSetter("abstractEventDispatcher")
        public void setAbstractEventDispatcher(String className) throws ClassNotFoundException {
            this.abstractEventDispatcher = (Class<? extends AbstractEventDispatcher>) Class.forName(className);
        }

        /**
         * Returns the class of the utility used to provide connection to a particular broker system.
         * @return A class object or a null.
         */
        public Class<? extends AbstractEventDispatcher> getAbstractEventDispatcher() {
            return abstractEventDispatcher;
        }

        /**
         * Used to set misc configuration data that is usually redirected to the broker instance during instantiation.
         * @param key The key used as identifier.
         * @param value The value associated with that key.
         */
        @JsonAnySetter
        public void setConfigurationData(String key, String value) {
            if (configurationData == null) configurationData = new Properties();
            configurationData.put(key, value);
        }

        /**
         * Returns misc configuration data that is usually redirected to the broker instance during instantiation.
         * @return An nonempty structure or null.
         */
        public Properties getConfigurationData() {
            return configurationData;
        }
    }

    /**
     * Contains settings to the various broker systems to which a connection will be established.
     */
    public List<Link> link;

    /**
     *  Constructor.
     */
    public BasicConfiguration() { }

    /**
     * Copy constructor.
     * @param cfg Another {@link BasicConfiguration} instance from which to copy the data. Providing null value instead
     *            is equivalent to calling the default constructor of the class.
     */
    public BasicConfiguration(BasicConfiguration cfg) {
        if (cfg == null) return;
        this.dataEncodingMechanism = cfg.dataEncodingMechanism;
        this.dispatchingType = cfg.dispatchingType;
        this.topic = cfg.topic;
        this.maxFailedDistributionAttempts = cfg.maxFailedDistributionAttempts;
        this.eventDeduplicationCapacity = cfg.eventDeduplicationCapacity;
        this.dispatchIntervalMillis = cfg.dispatchIntervalMillis;
        this.maxSubscribersLimit = cfg.maxSubscribersLimit;
        this.subscriberAliveCheckIntervalMillis = cfg.subscriberAliveCheckIntervalMillis;
        this.maxTimeWithoutBrokerConnectionMillis = cfg.maxTimeWithoutBrokerConnectionMillis;
        if (cfg.link != null) {
            this.link = new ArrayList<>(cfg.link.size());
            cfg.link.forEach(l -> link.add(new Link(l)));
        }
    }

    /**
     * Returns how often the agent will check for events pending for dispatching.
     * @return The amount of time in milliseconds on which the checking will be done.
     */
    public long getDispatchIntervalMillis() {
        return dispatchIntervalMillis;
    }

    /**
     * Sets how often the agent will check for events pending for dispatching.
     * @param dispatchIntervalMillis The amount of time in milliseconds on which the checking will be done.
     *                               The value must be greater than 0. By default it's set to 300.
     * @throws IllegalArgumentException If dispatchIntervalMillis is less than or equal to 0.
     */
    public void setDispatchIntervalMillis(long dispatchIntervalMillis) {
        if (dispatchIntervalMillis <= 0) {
            throw new IllegalArgumentException("Setting 'dispatchIntervalMillis' cannot be less or equal to zero!");
        }
        this.dispatchIntervalMillis = dispatchIntervalMillis;
    }

    /**
     * Creates instances of the event dispatchers handling the relaying to the specified broker systems.
     * @param linkModifier Optional temporary modifier of the Link parameters data, used during the instantiation of
     *                     the corresponding dispatchers. Can be null.
     * @param result Instance of the variable holding the results. Cannot be null. The result count might differ from
     *               the one in {@link #link} in case of errors.
     * @throws NullPointerException If result is null.
     */
    protected void makeDispatchers(Consumer<Link> linkModifier, List<IEventDispatcher> result) {
        Objects.requireNonNull(result, "Result holder must not be null");
        if (link == null || link.isEmpty()) return;

        Set<String> topics = new HashSet<>();
        topics.add(topic);

        Logger logger = null;

        for (Link link : this.link) {
            try {
                if (linkModifier != null) {
                    link = new Link(link);
                    linkModifier.accept(link);
                }

                if (EventDispatcherActiveMQ.class.isAssignableFrom(link.abstractEventDispatcher)) {
                    ConfigurationFactoryActiveMQ cfg = new ConfigurationFactoryActiveMQ(link.configurationData,
                            this.dataEncodingMechanism, this.dispatchingType, topics, null);

                    result.add(new EventDispatcherActiveMQ(cfg, link.latestEventsRememberCapacity,
                            link.doNotReceiveEventsFromSameSource, link.beRetroactive, null));

                } else if (EventDispatcherKafka.class.isAssignableFrom(link.abstractEventDispatcher)) {
                    if (link.beRetroactive) {
                        if (link.configurationData == null) link.configurationData = new Properties();
                        if (!link.configurationData.containsKey("auto.offset.reset")) {
                            link.configurationData.put("auto.offset.reset", "earliest");
                        }
                    }
                    ConfigurationFactoryKafka cfg = new ConfigurationFactoryKafka(link.configurationData,
                            this.dataEncodingMechanism, this.dispatchingType, topics, null);
                    result.add(new EventDispatcherKafka(cfg, link.latestEventsRememberCapacity,
                            link.doNotReceiveEventsFromSameSource, null));
                }
            } catch (Exception e) {
                if (logger == null) logger = Logger.getJADELogger(getClass().getName());
                logger.log(Logger.SEVERE, e.getMessage(), e);
            }
        }
    }

    /**
     * Synchronously creates instances of the event dispatchers handling the relaying to the specified broker systems.
     * @param linkModifier Optional temporary modifier of the Link parameters data, used during the instantiation of
     *                     the corresponding dispatchers. Can be null.
     * @return A list that might contain 0 or more instances. Because whether a dispatcher instance will be created and
     *         added to the list are 2 separate independent actions, that however depend on the underlying broker system
     *         implementation it is a good practice to compare the result count to that of the originally specified
     *         {@link #link} entries.
     */
    public List<IEventDispatcher> makeDispatchers(Consumer<Link> linkModifier) {
        int sz = 1;
        if (this.link != null) sz = this.link.size();
        List<IEventDispatcher> result = Collections.synchronizedList(new ArrayList<>(sz));
        this.makeDispatchers(linkModifier, result);
        return result;
    }

    /**
     * Asynchronously creates instances of the event dispatchers handling the relaying to the specified broker systems.
     * For the complete check {@link #makeDispatchers(Consumer)} method.
     * @param linkModifier Optional temporary modifier of the Link parameters data, used during the instantiation of
     *                     the corresponding dispatchers. Can be null.
     * @param result Instance of the variable holding the results. Using {@link Collections#synchronizedList(List)}
     *               to wrap the actual instance might be a good idea. Cannot be null.
     * @param executor Instance of {@link ExecutorService} for starting the asynchronous code execution. Cannot be null.
     * @return Instance of the FutureTask that will eventually do the creation and return when the process is done.
     * @throws NullPointerException If result or executor is null.
     */
    public FutureTask<List<IEventDispatcher>> makeDispatchers(
            Consumer<Link> linkModifier, List<IEventDispatcher> result, ExecutorService executor) {
        Objects.requireNonNull("Result holder must not be null");
        Objects.requireNonNull(executor, "ExecutorService must not be null");

        FutureTask<List<IEventDispatcher>> task = new FutureTask<>(() -> makeDispatchers(linkModifier, result), result);
        executor.execute(task);

        return task;
    }
}
