package net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology;

import jade.content.Concept;
import jade.content.onto.annotations.Slot;

import java.util.List;
import java.util.Objects;

/**
 * Subscription parameters passed to Event Broker agent.
 */
public class SubscriptionParameter implements Concept {
    private static final long serialVersionUID = 504036955411590743L;

    private String personalId;
    private String personalId2;
    private List<HashableProperty> otherParameters;

    /**
     * Constructor.
     */
    public SubscriptionParameter() { }

    /**
     * Constructor.
     * @param personalId Agent's personal id used for unique persistent identification in front of Event Source.
     *                   Can be null.
     * @param personalId2 Agent's second personal id used for unique persistent identification in front of Event Source.
     *                    Cam be null.
     * @param otherParameters Miscellaneous list of key - value parameters without fixed format. Can be null.
     */
    public SubscriptionParameter(String personalId, String personalId2, List<HashableProperty> otherParameters) {
        this.personalId = personalId;
        this.personalId2 = personalId2;
        this.otherParameters = otherParameters;
    }

    /**
     * Returns the current personal identifier (if any).
     * @return Any possible value.
     */
    @Slot(documentation = "Agent's personal id used for unique persistent identification in front of Event Source"
            + " (Broker) agents", manageAsSerializable = true, name = "personal-id", position = 0)
    public String getPersonalId() {
        return personalId;
    }

    /**
     * Sets the current personal identifier.
     * @param personalId Any possible value.
     */
    public void setPersonalId(String personalId) {
        this.personalId = personalId;
    }

    /**
     * Returns the current personal identifier 2 (if any).
     * @return Any possible value.
     */
    @Slot(documentation = "Agent's personal id 2 used for unique persistent identification in front of Event Source"
        + " (Broker) agents", manageAsSerializable = true, name = "personal-id-2", position = 1)
    public String getPersonalId2() {
        return personalId2;
    }

    /**
     * Sets the current personal identifier 2.
     * @param personalId2 Any possible value.
     */
    public void setPersonalId2(String personalId2) {
        this.personalId2 = personalId2;
    }

    /**
     * Returns the current miscellaneous parameters (if any).
     * @return Any possible value.
     */
    @Slot(documentation = "Miscellaneous list of key - value parameters without fixed format",
            manageAsSerializable = true, name = "other-parameters", position = 2)
    public List<HashableProperty> getOtherParameters() {
        return otherParameters;
    }

    /**
     * Sets the current miscellaneous parameters (if any).
     * @param otherParameters Any possible value.
     */
    public void setOtherParameters(List<HashableProperty> otherParameters) {
        this.otherParameters = otherParameters;
    }

    @Override
    public int hashCode() {
        int i = 17;
        i = 19 * i + (personalId != null ? personalId.hashCode() : 0);
        i = 19 * i + (otherParameters != null ? otherParameters.hashCode() : 0);
        return i;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof SubscriptionParameter) {
            SubscriptionParameter sp = (SubscriptionParameter) obj;
            return (Objects.equals(personalId, sp.personalId) && Objects.equals(personalId2, sp.personalId2)
                    && Objects.equals(otherParameters, sp.otherParameters));
        }
        return false;
    }
}
