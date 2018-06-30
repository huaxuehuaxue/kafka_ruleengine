/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datamelt.util;

public class Constants
{
	public static final String PROPERTY_KAFKA_BROKERS							= "kafka.brokers";
	public static final String PROPERTY_KAFKA_GROUP_ID							= "kafka.group.id";
	public static final String PROPERTY_KAFKA_CONSUMER_POLL						= "kafka.consumer.poll";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE						= "kafka.topic.source";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT				= "kafka.topic.source.format";
	public static final String PROPERTY_KAFKA_TOPIC_TARGET						= "kafka.topic.target";
	public static final String PROPERTY_KAFKA_TOPIC_TARGET_FAILED				= "kafka.topic.target.failed";
	public static final String PROPERTY_KAFKA_TOPIC_TARGET_LOGGING				= "kafka.topic.target.logging";
	public static final String PROPERTY_RULEENGINE_FAILED_MODE					= "ruleengine.failed.mode";
	public static final String PROPERTY_RULEENGINE_FAILED_NUMBER_OF_GROUPS		= "ruleengine.failed.minimum_number_of_groups";
	public static final String PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL		= "ruleengine.check.modified.file.interval";
	public static final String PROPERTY_KAFKA_MESSAGE_FORMAT					= "kafka.topic.source.format";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS	= "kafka.topic.source.format.csv.fields";
	public static final String PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR	= "kafka.topic.source.format.csv.value.separator";
	
	// separator used in the properties file when multiple values are specified
	public static final String PROPERTY_VALUES_SEPARATOR						= ",";
	
	public static final String PROPERTY_KAFKA_BOOTSTRAP_SERVERS					= "bootstrap.servers";
	public static final String PROPERTY_KAFKA_CONSUMER_GROUP_ID					= "group.id";
	
	public static final String RULEENGINE_FIELD_GROUP_ID					= "ruleengine_group";
	public static final String RULEENGINE_FIELD_GROUP_FAILED				= "ruleengine_group_failed";
	public static final String RULEENGINE_FIELD_SUBGROUP_ID					= "ruleengine_subgroup";
	public static final String RULEENGINE_FIELD_SUBGROUP_FAILED				= "ruleengine_subgroup_failed";
	public static final String RULEENGINE_FIELD_SUBGROUP_OPERATOR			= "ruleengine_subgroup_operator";
	public static final String RULEENGINE_FIELD_RULES_OPERATOR				= "ruleengine_rules_operator";
	public static final String RULEENGINE_FIELD_RULE_ID						= "ruleengine_rule";
	public static final String RULEENGINE_FIELD_RULE_FAILED					= "ruleengine_rule_failed";
	public static final String RULEENGINE_FIELD_RULE_MESSAGE				= "ruleengine_rule_message";
	
	public static final String LEVEL_INFO									= "INFO";
	public static final String LEVEL_WARNING								= "WARNING";
	public static final String LEVEL_ERROR									= "ERROR";
	
	public static final String MESSAGE_FORMAT_JSON							= "json";
	public static final String MESSAGE_FORMAT_AVRO							= "avro";
	public static final String MESSAGE_FORMAT_CSV							= "csv";
	
    public static final int RULEENGINE_OUTPUT_MODE_ALL_GROUPS				= 0;
    public static final int RULEENGINE_OUTPUT_MODE_PASSED_GROUPS			= 1;
    public static final int RULEENGINE_OUTPUT_MODE_FAILED_GROUPS			= 2;
    

}
