#!/bin/bash
# script to run the RuleEngineConsumerProducer java program
#
# the program will read the properties from the given properties file.
# Then messages from the kafka source topic are read, processed using
# the ruleengine project file and the result will be output to a kafka
# target topic. Additonally a topic may be specified to output the
# detailed results of the ruleengine execution.
#
# last update: uwe.geercken@web.de - 2018-06-02


# path and name of the properties file to use. if undefined use the default name
properties_file="${1:-kafka_ruleengine.properties}"

# run the kafka ruleengine program
java -cp .:"lib/*" com.datamelt.kafka.ruleengine.RuleEngineConsumerProducer "${properties_file}"

