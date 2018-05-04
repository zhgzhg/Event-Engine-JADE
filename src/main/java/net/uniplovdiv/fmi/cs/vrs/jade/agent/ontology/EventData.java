package net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology;

import jade.content.Concept;
import jade.content.onto.annotations.Slot;
import jade.core.AID;

import java.util.List;

/**
 * Represents concept for serialized event data.
 */
public class EventData implements Concept {
    private static final long serialVersionUID = -6493960002054241693L;
    private List<byte[]> data;
    private AID source;

    /**
     * Returns the current list of serialized event data.
     * @return A list that might be null or containing 0 or more arrays of bytes.
     */
    @Slot(position = 0, documentation = "List of serialized byte arrays representing events", mandatory = true)
    public List<byte[]> getData() {
        return data;
    }

    /**
     * Returns the current list of serialized event data.
     * @param data A list that might be null or containing 0 or more arrays of bytes.
     */
    public void setData(List<byte[]> data) {
        this.data = data;
    }

    /**
     * Returns the possible (not very reliable) agent identifier from which the event data originates.
     * @return An instance or null.
     */
    @Slot(documentation = "The agent identifier from which the event data originates")
    public AID getSource() {
        return source;
    }

    /**
     * Sets the agent identifier, source of the event data.
     * @param source An agent identifier instance or null.
     */
    public void setSource(AID source) {
        this.source = source;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 71 * result + (data != null ? data.hashCode() : 0);
        result = 71 * result + (source != null ? source.hashCode() : 0);
        return result;
    }
}
