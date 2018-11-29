Event Engine for JADE
=====================

![build status badge](https://travis-ci.org/zhgzhg/Event-Engine-JADE.svg?branch=master "Build Status") [ ![Download](https://api.bintray.com/packages/zhgzhg/Event-Engine/Event-Engine-JADE/images/download.svg "Download Latest Versoin") ](https://bintray.com/zhgzhg/Event-Engine/Event-Engine-JADE/0.2.0)

Library providing implementation for event broker agents, behaviours and event serialization utilities used for
distribution of events into JADE multi-agent environments.
This project utilizes [Event Engine](https://github.com/zhgzhg/Event-Engine "Event Engine") library - the core component
used to represent events. Every other agent that's interested to use such events is also required to integrate the
library (usually though this component) in order to be able to interpret the received event data.

The project is distributed under LGPLv3 or later license.


Requirements
------------

* Java 8+
* Maven 3.3.9+ or IntelliJ IDEA 2018.1+


Compilation
===========

### For Java 8:
* With Maven (recommended):
    * Execute `mvn clean install -P java8`
* With IntelliJ IDEA: 
    * Open the project and build it using the gui options. (no jar artifacts profiles)

### For Java 9+ - alpha, extremely limited modularization, not recommended:
* With Maven (recommended):
    * Execute `mvn clean install -P java9p`
* With IntelliJ IDEA (no jar artifacts profiles):
    * Open the project.
    * In project's settings specify Project JDK to be JDK9 
    * For every module copy the file module-info.java inside its java directory.


Execution
=========

The default event distribution agent can be executed though maven:

`mvn exec:java -P jade-agent`
    
A very simple event client agent with the default event distribution agent can be executed through maven as well:

`mvn exec:java -P jade-with-test-client-agent`
