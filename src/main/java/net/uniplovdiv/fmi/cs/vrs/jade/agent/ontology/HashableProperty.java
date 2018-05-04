package net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology;

import jade.domain.FIPAAgentManagement.Property;

import java.util.Objects;

/**
 * Extension of JADE's class Property that implements hashCode() and equals().
 */
public class HashableProperty extends Property {
    private static final long serialVersionUID = 2618889384558650446L;

    /**
     * Constructor.
     */
    public HashableProperty() { super(); }

    /**
     * Constructor.
     * @param name The name (key) identifier.
     * @param value The corresponding value.
     */
    public HashableProperty(String name, Object value) { super(name, value); }

    @Override
    public int hashCode() {
        int i = 31;
        i = 19 * i + (getName() != null ? getName().hashCode() : 0);
        i = 19 * i + (getValue() != null ? getValue().hashCode() : 0);
        return i;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof HashableProperty) {
            HashableProperty mp = (HashableProperty) obj;
            return (Objects.equals(getName(), mp.getName()) && Objects.equals(getValue(), mp.getValue()));
        }
        return false;
    }
}
