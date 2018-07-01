/**
 * Provides Event Engine functionality for JADE.
 */
module net.uniplovdiv.fmi.cs.vrs.jade {
    requires java.base;
    requires java.logging;
    requires java.naming;

    requires jade;
    requires org.slf4j;
    requires org.slf4j.simple;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.dataformat.javaprop;
    requires org.apache.commons.lang3;

    requires transitive net.uniplovdiv.fmi.cs.vrs.event;
    requires transitive net.uniplovdiv.fmi.cs.vrs.event.annotations;
    requires transitive net.uniplovdiv.fmi.cs.vrs.event.serializers;
    requires transitive net.uniplovdiv.fmi.cs.vrs.event.dispatchers;

    exports net.uniplovdiv.fmi.cs.vrs.jade.agent;
    exports net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour;
    exports net.uniplovdiv.fmi.cs.vrs.jade.agent.configuration;
    exports net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology;
    exports net.uniplovdiv.fmi.cs.vrs.jade.agent.util;

    opens net.uniplovdiv.fmi.cs.vrs.jade.agent to net.uniplovdiv.fmi.cs.vrs.event.annotations;
}