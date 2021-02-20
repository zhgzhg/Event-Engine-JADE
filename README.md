Event Engine for JADE
=====================
<img alt="Event Engine for JADE Logo" src="https://raw.githubusercontent.com/zhgzhg/Event-Engine-JADE/master/logo.svg?sanitize=true" height="160" width="160"/>

Library providing implementation for event broker agents, behaviours and event serialisation utilities used for
distribution and receiving events inside multi-agent environments based on
[JADE (Java Agent DEvelopment Framework)](https://jade.tilab.com/ "JADE website")

![build status badge](https://travis-ci.com/zhgzhg/Event-Engine-JADE.svg?branch=master "Build Status")


This project utilises the [Event Engine](https://github.com/zhgzhg/Event-Engine "Event Engine") library which is
the core component used to represent events. Every other agent that is interested to use such events is also
required to integrate it (usually via
[Event-Engine-JADE](https://github.com/zhgzhg/Event-Engine-JADE "Event Engine for JADE")) in order to be able
to interpret the received event data.

The project is distributed under LGPLv3 license or its later versions.


What's Provided
---------------
* Implementation of Event Broker Agent (that can be further extended if needed; based on JADE)
* JADE behaviours for automatic:
    * Subscription to event broker agents and subscription management
    * Announcement of event broker agents
    * Exchange of events via specialised channel
    * Garbage collection of agent messages
* JADE helper utilities for working with:
    * Service descriptions
    * Directory Facilitator / Yellow Pages


Requirements
------------

* Java 8+
* Maven 3.6.2+ or IntelliJ IDEA 2019.1+
* JADE v4.5.0+ (older versions might work too, not tested)


Compilation
===========

### For Java 8:
* With Maven (recommended):
    * Execute `mvn clean install -P java8`
* With IntelliJ IDEA: 
    * Open the project and build it using the GUI options. (no profiles for jar artifacts)

### For Java 9+ - beta, limited modularization:
* With Maven (recommended):
    * Execute `mvn clean install -P java9p`
* With IntelliJ IDEA (no profiles for jar artifacts):
    * Open the project.
    * In project's settings specify Project JDK to be JDK9 or later
    * For every module copy the file module-info.java inside its java directory.


Execution
=========

The default event distribution agent can be executed through maven:

`mvn exec:java -P jade-agent`
    
A very simple event client agent with the default event distribution agent can be executed through maven as well:

`mvn exec:java -P jade-with-test-client-agent`

How To Use
==========

At this point the examples are extremely limited, so please refer to the pom.xml file, the aforementioned profiles and
dispatcher.properties file holding the default configuration of the event broker agent.

First include the library into your project. Click on the download links at the top to see how.
Then you will need an event broker agent (to run one check the pom.xml) connected to a message broker system(s) (a quick
and easy choice without any configuration is Apache ActiveMQ). Sample code for ordinary (client) agents (demonstrating
only the basic capabilities of the library - how to send and receive events) can be found inside the pseudo test client
agent.
