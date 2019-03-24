#!/bin/bash
sed -i "/<profiles>/ a\
    <profile>\
        <repositories>\
            <repository>\
                <snapshots>\
                    <enabled>false</enabled>\
                </snapshots>\
                <id>bintray-zhgzhg-Event-Engine</id>\
                <name>bintray</name>\
                <url>https://dl.bintray.com/zhgzhg/Event-Engine</url>\
            </repository>\
        </repositories>\
        <pluginRepositories>\
            <pluginRepository>\
                <snapshots>\
                    <enabled>false</enabled>\
                </snapshots>\
                <id>bintray-zhgzhg-Event-Engine</id>\
                <name>bintray-plugins</name>\
                <url>https://dl.bintray.com/zhgzhg/Event-Engine</url>\
            </pluginRepository>\
        </pluginRepositories>\
        <id>bintray</id>\
        <activation>\
            <activeByDefault>true</activeByDefault>\
        </activation>\
    </profile>" ~/.m2/settings.xml
cat ~/.m2/settings.xml



