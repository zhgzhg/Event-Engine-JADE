package net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology;

import jade.content.onto.BasicOntology;
import jade.content.onto.BeanOntology;
import jade.content.onto.BeanOntologyException;
import jade.util.Logger;

import java.util.logging.Level;

/**
 * The ontology used to subscribe for and distribute Event Engine's events.
 */
public class EventEngineOntology extends BeanOntology {
    private static final long serialVersionUID = 3546777599623601985L;

    /**
     * The name of the ontology.
     */
    public static final String NAME = "event-engine-ontology";

    private static EventEngineOntology theInstance = new EventEngineOntology();

    /**
     * Constructor.
     */
    private EventEngineOntology() {
        super(NAME, BasicOntology.getInstance());
        try {
            // all classes within this package will be included as part of the ontology
            add(getClass().getPackage().getName());
        } catch (BeanOntologyException e) {
            Logger.getJADELogger(getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * Returns an instance of this ontology.
     * @return An EventEngineOntology instance.
     */
    public static EventEngineOntology getInstance() {
        return theInstance;
    }
}
