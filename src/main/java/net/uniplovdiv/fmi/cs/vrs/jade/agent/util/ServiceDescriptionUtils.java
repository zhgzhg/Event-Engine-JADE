package net.uniplovdiv.fmi.cs.vrs.jade.agent.util;

import jade.domain.FIPAAgentManagement.Property;
import jade.domain.FIPAAgentManagement.ServiceDescription;
import jade.domain.FIPANames;
import jade.util.leap.Iterator;
import net.uniplovdiv.fmi.cs.vrs.event.Event;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour.BEventChannel;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.EventEngineOntology;

import java.util.Objects;

/**
 * Provides common default routines for constructing ServiceDescription(s).
 */
public class ServiceDescriptionUtils {
    /**
     * The name of the service that will be registered in the directory facilitator.
     */
    public static final String SERVICE_NAME = "event-broker";
    /**
     * The type of the service that will be registered in the directory facilitator.
     */
    public static final String SERVICE_TYPE = "event-engine-jade";
    /**
     * The property name pointing to which event topic will be used.
     */
    public static final String EVENT_TOPIC_PROP = "event-topic";
    /**
     * Property to be used in the directory facilitator to indicate that the event source accepts new subscribers.
     * For corresponding values see {@link #ACCEPT_NEW_SUBSCRIBERS_AFFIRMATIVE_ANS} and
     * {@link #ACCEPT_NEW_SUBSCRIBERS_NEGATIVE_ANS}.
     */
    public static final String ACCEPT_NEW_SUBSCRIBERS = "accept-new-subscribers";
    /**
     * Corresponding value of {@link #ACCEPT_NEW_SUBSCRIBERS} of meaning that new subscribers are accepted.
     */
    public static final String ACCEPT_NEW_SUBSCRIBERS_AFFIRMATIVE_ANS = "true";
    /**
     * Corresponding sample value of {@link #ACCEPT_NEW_SUBSCRIBERS} of meaning that new subscribers are not accepted.
     */
    public static final String ACCEPT_NEW_SUBSCRIBERS_NEGATIVE_ANS = "false";

    /**
     * Creates default service description for Event Source agents. The descriptor should be adjusted afterwards for
     * fine tuning.
     * @param topic The event type that the Event Source agent will only work with. If null will use the default one.
     * @return An initialized with default data ServiceDescription instance.
     */
    public static ServiceDescription createEventSourceSD(String topic) {
        ServiceDescription sd = new ServiceDescription();
        sd.setName(SERVICE_NAME);
        sd.setType(SERVICE_TYPE);

        sd.addProtocols(FIPANames.InteractionProtocol.FIPA_SUBSCRIBE);
        sd.addProtocols(BEventChannel.EVENT_EXCHANGE_PROTOCOL_NAME);

        sd.addProperties(new Property(EVENT_TOPIC_PROP,
                (topic == null || topic.isEmpty() ? new Event().getCategory() : topic)));
        sd.addProperties(new Property(ACCEPT_NEW_SUBSCRIBERS, ACCEPT_NEW_SUBSCRIBERS_AFFIRMATIVE_ANS));
        sd.addOntologies(EventEngineOntology.NAME);
        sd.addLanguages(FIPANames.ContentLanguage.FIPA_SL);
        return sd;
    }

    /**
     * Searches {@link ServiceDescription} instance's properties for a particular property by name and returns the first
     * match.
     * @param name The name of the property.
     * @param sd The {@link ServiceDescription} instance to be searched.
     * @return The property instance if found, otherwise null.
     */
    public static jade.domain.FIPAAgentManagement.Property findFirstPropertyNamed(String name, ServiceDescription sd) {
        if (sd != null) {
            synchronized (sd) {
                Iterator propsIterator = sd.getAllProperties();
                if (propsIterator != null) {
                    while (propsIterator.hasNext()) {
                        Property p = (Property) propsIterator.next();
                        if (p != null) {
                            String pName = p.getName();
                            if (Objects.equals(name, pName)) {
                                return p;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Adds or changes the first match of {@link ServiceDescription} instance's property by name criterion.
     * @param name The name of the property (string key).
     * @param value The new value to be set.
     * @param sd The {@link ServiceDescription} instance to be searched. Cannot be null.
     * @return The previous value of the property found or null.
     * @throws NullPointerException If sd is null.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static Object setFirstPropertyNamed(String name, Object value, ServiceDescription sd) {
        Objects.requireNonNull(sd, "ServiceDescription cannot be null");
        synchronized (sd) {
            Property existingProperty = findFirstPropertyNamed(name, sd);
            if (existingProperty == null) {
                sd.addProperties(new Property(name, value));
                return null;
            }
            Object oldValue = existingProperty.getValue();
            existingProperty.setName(name);
            existingProperty.setValue(value);
            return oldValue;
        }
    }

    /**
     * Removes the first found property from {@link ServiceDescription} matching the specified name criterion.
     * @param name The name of the property (string key).
     * @param sd The {@link ServiceDescription} instance to be searched.
     * @return The removed property.
     */
    public static jade.domain.FIPAAgentManagement.Property removeFirstPropertyNamed(String name,
                                                                                    ServiceDescription sd) {
        if (sd != null) {
            synchronized (sd) {
                Iterator propsIterator = sd.getAllProperties();
                if (propsIterator != null) {
                    while (propsIterator.hasNext()) {
                        Property p = (Property) propsIterator.next();
                        if (p != null && Objects.equals(name, p.getName())) {
                            propsIterator.remove();
                            return p;
                        }
                    }
                }
            }
        }
        return null;
    }
}
