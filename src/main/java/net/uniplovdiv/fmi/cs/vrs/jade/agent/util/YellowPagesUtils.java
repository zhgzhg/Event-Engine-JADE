package net.uniplovdiv.fmi.cs.vrs.jade.agent.util;

import jade.core.AID;
import jade.core.Agent;
import jade.domain.DFService;
import jade.domain.FIPAAgentManagement.*;
import jade.domain.FIPAException;
import jade.lang.acl.ACLMessage;
import jade.util.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;

/**
 * Provides functionality for de/registration in JADE's Directory Facilitator (yellow pages).
 */
public class YellowPagesUtils {
    private volatile boolean isRegisteredInDF = false;
    private Agent agent;
    private volatile ServiceDescription sd;
    private volatile DFAgentDescription dfd;
    private final Logger logger;

    /**
     * Constructor.
     * @param agent The agent to work with. Cannot be null.
     * @param sd Description of the agent's services. For e.g. language, ontology, protocols, etc. Cannot be null.
     *           Needs to be initialized. Instance with the default values can be obtained using
     *           {@link ServiceDescriptionUtils#createEventSourceSD(String)} method.
     * @throws NullPointerException If agent or service description is null.
     */
    public YellowPagesUtils(Agent agent, ServiceDescription sd) {
        Objects.requireNonNull(agent, "Null agent");
        Objects.requireNonNull(sd, "Null service description");
        this.agent = agent;
        this.sd = sd;
        this.logger = Logger.getJADELogger(agent.getClass().getName());
    }

    /**
     * Registers agent into the Directory Facilitator (yellow pages service). Registering an already registered instance
     * will cause it to immediately return true.
     * @return True if the registration was successful otherwise false.
     */
    @SuppressWarnings("UnusedReturnValue")
    public synchronized boolean register() {
        try {
            if (this.isRegisteredInDF) {
                return true;
            }

            DFAgentDescription dfd = new DFAgentDescription();
            dfd.setName(this.agent.getAID());

            dfd.addServices(sd);
            this.dfd = DFService.register(this.agent, dfd);
            this.isRegisteredInDF = true;
        } catch (FIPAException fe) {
            this.isRegisteredInDF = false;
            this.logger.log(Logger.SEVERE, fe.getMessage(), fe);
        }
        return this.isRegisteredInDF;
    }

    /**
     * Deregisters agent from the directory facilitator (yellow pages service).
     * @return True if the deregistration is successful otherwise false
     */
    @SuppressWarnings("UnusedReturnValue")
    public synchronized boolean deregister() {
        try {
            if (this.isRegisteredInDF) {
                // DFService.deregister(this.agent, this.dfd); // too unreliable because it's blocking for long.
                // Also on suspend the messages don't get received
                ACLMessage request = DFService.createRequestMessage(this.agent, this.agent.getDefaultDF(),
                        "deregister", dfd, null);
                DFService.doFipaRequestClient(this.agent, request, 30000);
                this.isRegisteredInDF = false;
            }
        } catch (FIPAException fe) {
            this.logger.log(Logger.SEVERE, fe.getMessage(), fe);
        }
        return !this.isRegisteredInDF;
    }

    /**
     * Returns the current service descriptor for the particular agent associated with this utility.
     * @return An initialized service descriptor.
     */
    public ServiceDescription getServiceDescription() {
        return this.sd;
    }

    /**
     * Sets new service descriptor for the particular agent associated with this utility. Already registered agents
     * must be re-registered in order the directory facilitator to take note of the changes.
     * @param sd The new service descriptor. Cannot be null.
     * @throws NullPointerException If agent or service description is null.
     */
    public synchronized void setServiceDescription(ServiceDescription sd) {
        Objects.requireNonNull(sd, "Null service description");
        this.sd = sd;
    }

    /**
     * Returns information whether the current agent instance associated with this utility has been successfully
     * registered into the Directory Facilitator.
     * @return True if it has been registered. Otherwise false.
     */
    public boolean isRegisteredInDF() {
        return isRegisteredInDF;
    }

    /**
     * Returns the agent associated with the current instance of this utility.
     * @return The instance of the associated JADE agent.
     */
    public Agent getAgent() {
        return this.agent;
    }

    /**
     * Searches in Directory Facilitator for agents matching the service description, the current instance was
     * initialized with.
     * @return List containing the agent descriptions. If nothing is found the list will be empty.
     */
    public List<DFAgentDescription> searchEx() {
        List<DFAgentDescription> agents = new ArrayList<>();
        try {
            DFAgentDescription template = new DFAgentDescription();
            template.addServices(this.sd);
            DFAgentDescription[] results = DFService.search(this.agent, template);
            if (results != null && results.length > 0) {
                for (DFAgentDescription r : results) {
                    agents.add(r);
                }
            }
        } catch (Exception e) {
            Logger.getJADELogger(getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
        }
        return agents;
    }

    /**
     * Searches in Directory Facilitator for agents matching the service description, the current instance was
     * initialized with.
     * @return List containing the agents matching the desired services. If nothing is found the list will be empty.
     */
    public List<AID> search() {
        List<AID> agents = new ArrayList<>();
        this.searchEx().forEach(agentDescr -> agents.add(agentDescr.getName()));
        return agents;
    }
}