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

package com.datamelt.kafka.ruleengine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import com.datamelt.rules.core.ReferenceField;
import com.datamelt.rules.core.RuleExecutionResult;
import com.datamelt.rules.core.RuleGroup;
import com.datamelt.rules.core.RuleSubGroup;
import com.datamelt.rules.core.XmlRule;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.Constants;
import com.datamelt.util.FormatConverter;
import com.datamelt.util.PropertiesFileException;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;

/**
 * Reads JSON formatted data from a Kafka topic, runs business rules (logic) on the data and
 * outputs the resulting data to a Kafka target topic.
 * 
 * Optionally the detailed results of the execution of the ruleengine may be ouput to a defined
 * topic for logging purposes. In this case the topic will contain one output message for each
 * input message and rule. E.g. if there are 10 rules in the ruleengine project file, then for any
 * received input message, 10 ouput messages are generated.
 * 
 * The source topic data is expected to be in JSON format. Output will be in JSON format.
 * 
 * @author uwe geercken - 2018-05-30
 *
 */
public class RuleEngineConsumerProducer
{
	// consumer properties
	private static final String enableAutoCommit 					= "true";
	private static final String autoCommitIntervalMs 				= "1000";
	private static final String keyDeserializer 					= "org.apache.kafka.common.serialization.StringDeserializer";
	private static final String valueDeserializer 					= "org.apache.kafka.common.serialization.StringDeserializer";
	private static final int 	numberOfRecordsToPoll 				= 100;
	private static final String autoOffsetReset						= "earliest";
	
	// producer properties
	private static final String acks 								=  "all";
	private static final String retries 							= "0";
	private static final String batchSize 							= "16384";
	private static final String lingerMs 							= "1";
	private static final String bufferMemory 						= "67108864";
	private static final String keySerializer 						= "org.apache.kafka.common.serialization.StringSerializer";
	private static final String valueSerializer 					= "org.apache.kafka.common.serialization.StringSerializer";
	
	private BusinessRulesEngine ruleEngine;
	private ArrayList<RowField> ruleEngineProjectFileReferenceFields = new ArrayList<RowField>();
	private boolean referenceFieldsCollected 						 = false;
	private Properties properties 									 = new Properties();
	private SimpleDateFormat sdf 									 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private boolean outputToFailedTopic								 = false;
	private int failedMode										 	 = 0;
	private int failedNumberOfGroups							 	 = 0;
	
	private static volatile boolean keepRunning = true;
	
	public RuleEngineConsumerProducer (String propertiesFileName) throws Exception
	{
		// load properties file
		loadProperties(propertiesFileName);
		
		// get a reference to the ruleengine project zip file containing the business rules
		ZipFile ruleengineProjectZipFile = getRuleEngineProjectZipFile(getProperty(Constants.PROPERTY_RULEENGINE_PROJECT_FILE));
			
		// initialize the ruleengine with the ruleengine project zip file
		this.ruleEngine = new BusinessRulesEngine(ruleengineProjectZipFile);

		// we do not want to preserve the detailed results of the ruleengine execution
		// if we are loggin the detailed results to a topic
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).equals(""))
		{
			this.ruleEngine.setPreserveRuleExcecutionResults(false);
		}
		
		// determine the failed mode. can be "at least one" or "all" rulegroups failed 
		// has to be according the RULEGROUP_STATUS_MODE... of the ruleengine
		if(getProperty(Constants.PROPERTY_RULEENGINE_FAILED_MODE)!=null && !getProperty(Constants.PROPERTY_RULEENGINE_FAILED_MODE).equals(""))
		{
			try
			{
				failedMode = Integer.parseInt(getProperty(Constants.PROPERTY_RULEENGINE_FAILED_MODE));
			}
			catch(Exception ex)
			{
			}
		}
		
		// determine the number of groups that must have failed so that
		// the data is regarded as failed
		if(failedMode==0)
		{
			if(getProperty(Constants.PROPERTY_RULEENGINE_FAILED_NUMBER_OF_GROUPS)!=null && !getProperty(Constants.PROPERTY_RULEENGINE_FAILED_NUMBER_OF_GROUPS).equals(""))
			{
				try
				{
					failedNumberOfGroups = Integer.parseInt(getProperty(Constants.PROPERTY_RULEENGINE_FAILED_NUMBER_OF_GROUPS));
				}
				catch(Exception ex)
				{
				}
			}
		}

		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).equals(""))
		{
			outputToFailedTopic = true;
		}
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
		    public void run() {
	        	System.out.println(getSystemMessage(Constants.LEVEL_INFO,"program shutdown..."));
		        try
		        {
			    	keepRunning = false;
		        }
		        catch(Exception ex)
		        {
		        	ex.printStackTrace();
		        }
		    }
		});
	}
	
	public static void main(String[] args) throws Exception
	{
		if(args.length==0)
    	{
    		help();
    	}
		else
		{
			// create a RuleEngineConsumerProducer instance and run it
			try
			{
				RuleEngineConsumerProducer ruleEngineConsumerProducer = new RuleEngineConsumerProducer(args[0]);
				ruleEngineConsumerProducer.run();
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
		}
	}
	
	private static void help()
	{
		System.out.println("RuleEngineConsumerProducer. program to process data from an Apache Kafka source topic,");
    	System.out.println("run the business rules from a ruleengine project file against the data and output the");
    	System.out.println("results to an Apache kafka target topic. Additionally an optional topic for logging");
    	System.out.println("may be specified which will contain the detailed results of the execution of the ruleengine.");
    	System.out.println();
    	System.out.println("The Apache Kafka source topic messages must be in JSON format. Output will be also");
    	System.out.println("in JSON format");
    	System.out.println();
    	System.out.println("A properties file must be specified, defining various properties for the program.");
    	System.out.println();
    	System.out.println("RuleEngineConsumerProducer [properties file]");
    	System.out.println("where [properties file]   : required. path and name of the properties file");
    	System.out.println();
    	System.out.println("example: RuleEngineConsumerProducer /home/test/kafka_ruleengine.properties");
    	System.out.println();
    	System.out.println("published as open source under the Apache License. read the licence notice");
    	System.out.println("all code by uwe geercken, 2006-2018. uwe.geercken@web.de");
    	System.out.println();
	}
	
	private void loadProperties(String propertiesFilename) throws PropertiesFileException,FileNotFoundException,IOException
    {
    	File propertiesFile = new File(propertiesFilename);
    	if(!propertiesFile.exists())
    	{
    		throw new PropertiesFileException(getSystemMessage(Constants.LEVEL_ERROR,"properties file not found: [" + propertiesFilename + "]"));
    	}
    	else if(!propertiesFile.canRead())
    	{
    		throw new PropertiesFileException(getSystemMessage(Constants.LEVEL_ERROR,"properties file can not be read: [" + propertiesFilename + "]"));
    	}
    	else if(!propertiesFile.isFile())
    	{
    		throw new PropertiesFileException(getSystemMessage(Constants.LEVEL_ERROR,"properties file is not a file: [" + propertiesFilename + "]"));
    	}
    	else
    	{
    		FileInputStream inputStream = new FileInputStream(propertiesFile);
    		properties.load(inputStream);
    		inputStream.close();
    	}
    }
	
	private ZipFile getRuleEngineProjectZipFile(String filename) throws FileNotFoundException,IOException,ZipException
    {
    	File ruleengineFile = new File(filename);
		
    	if(!ruleengineFile.exists())
    	{
    		throw new FileNotFoundException(getSystemMessage(Constants.LEVEL_ERROR,"ruleengine project zip file not found: [" + filename + "]"));
    	}
    	else if(!ruleengineFile.canRead())
    	{
    		throw new FileNotFoundException(getSystemMessage(Constants.LEVEL_ERROR,"ruleengine project zip file can not be read: [" + filename + "]"));
    	}
    	else if(!ruleengineFile.isFile())
    	{
    		throw new FileNotFoundException(getSystemMessage(Constants.LEVEL_ERROR,"ruleengine project zip file is not a file: [" + filename + "]"));
    	}
    	else
    	{
    		ZipFile zipFile = new ZipFile(ruleengineFile);	
    		return zipFile;
    	}
    }
	
	/**
	 * returns a property by specifying the key of the property
	 * 
	 * @param key		the key of a property
	 * @return			the value of the property for the given key
	 */
	private String getProperty(String key)
	{
		return properties.getProperty(key);
	}
	
	/**
	 * runs the RuleEngineConsumerProducer program. Messages are read from an input topic, processed using the ruleengine
	 * and are output to the target topic.
	 * 
	 */
	public void run()
	{
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka brokers: " + getProperty(Constants.PROPERTY_KAFKA_BROKERS)));
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka source topic: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)));
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,"ruleengine project file: " + getProperty(Constants.PROPERTY_RULEENGINE_PROJECT_FILE)));
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,"ruleengine project reference fields: " + ruleEngine.getReferenceFields().toString()));
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka target topic: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)));
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka target topic failed: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)));
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka logging topic: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)));
		System.out.println(getSystemMessage(Constants.LEVEL_INFO,""));

		// used for counting the number of messages/records.
		// if no key is defined in the kafka message, then this value is used
		// as the label durin the ruleengine execution 
		long counter = 0;
        
		// create a consumer for the source topic
		try(
				KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(getConsumerProperties(getProperty(Constants.PROPERTY_KAFKA_BROKERS), getProperty(Constants.PROPERTY_KAFKA_GROUP_ID)));
				KafkaProducer<String, String>kafkaProducer = new KafkaProducer<>(getProducerProperties(getProperty(Constants.PROPERTY_KAFKA_BROKERS)));
				KafkaProducer<String, String>kafkaProducerLogging = new KafkaProducer<>(getProducerProperties(getProperty(Constants.PROPERTY_KAFKA_BROKERS)));
				KafkaProducer<String, String>kafkaProducerFailed = new KafkaProducer<>(getProducerProperties(getProperty(Constants.PROPERTY_KAFKA_BROKERS)))
			)
		{
			// subscribe to the given kafka topic
			kafkaConsumer.subscribe(Arrays.asList(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)));
			
			// process while we receive messages and if we haven't encountered an exception
			while (true && keepRunning) 
	        {
				// poll records from kafka
	        	ConsumerRecords<String, String> records = kafkaConsumer.poll(numberOfRecordsToPoll);
			
	        	// loop over the records/messages
				for (ConsumerRecord<String, String> record : records) 
				{
					counter++;
					try
					{
						// create a collection of fields from the incoming record
						RowFieldCollection collection = FormatConverter.convertFromJson(record.value());
						
						// add fields that have been additionally defined in the ruleengine project file
						// these are fields that are not present in the message but used by the rule engine.
						// after the first message was read, the fields are cached for better performance.
						addReferenceFields(collection);
					
						// label for the ruleengine. used for assigning a unique label to each record
						// this is usefull when outputting the results.
						String label = getLabel(record.key(), counter);
						
						// run the ruleengine
						ruleEngine.run(label, collection);

						// format the values from the rowfield collection as json
						JSONObject jsonMessage = FormatConverter.convertToJson(collection); 
						
						// if we don't have a topic specified for the failed messages only then all
						// messages are sent to the target topic
						if(!outputToFailedTopic)
						{
							// send the resulting data to the target topic
							sendTargetTopicMessage(kafkaProducer, record.key(), jsonMessage);
						}
						else
						{
							// depending on the selected mode the ruleengine returns if
							// this status is true/valid
							if(failedMode != 0 && ruleEngine.getRuleGroupsStatus(failedMode))
							{
								// send the message to the target topic for failed messages
								sendFailedTargetTopicMessage(kafkaProducerFailed, record.key(), jsonMessage);
							}
							// if failedMode is equal to 0, then the user specifies the minimum number
							// of groups that must have failed to regard the data as failed
							else if(failedMode == 0 && ruleEngine.getRuleGroupsMinimumNumberFailed(failedNumberOfGroups)) 
							{
								// send the message to the target topic for failed messages
								sendFailedTargetTopicMessage(kafkaProducerFailed, record.key(), jsonMessage);
							}
							else
							{
								// otherwise send message to target topic
								sendTargetTopicMessage(kafkaProducer, record.key(), jsonMessage);
							}
						}

						// send the rule execution details as message(s) to the logging topic, if one is defined.
						// if no logging topic is defined, then no execution details will be generated 
						// as ruleEngine.setPreserveRuleExcecutionResults() is set to "false"
						if(ruleEngine.getPreserveRuleExcecutionResults())
						{
							sendLoggingTargetTopicMessage(kafkaProducerLogging, record.key(), jsonMessage, ruleEngine);
							
							// clear the collection of details/result
							ruleEngine.getRuleExecutionCollection().clear();
						}
					}
					// if we have a parsing problem with the JSON message, we continue processing
					catch(JSONException jex)
					{
						System.out.println(getSystemMessage(Constants.LEVEL_ERROR, "error parsing message: [" + record.value() + "]"));
					}
					// if we have any other exception we shutdown
					catch(Exception ex)
					{
						ex.printStackTrace();
						keepRunning=false;
					}
				}
			}
		}
	}
	
	/**
	 * Returns the label used by the rulengine for each record.
	 * 
	 * @param recordKey		key of the kafka message
	 * @param counter		the current counter for the number of messages retrieved
	 * @return
	 */
	private String getLabel(String recordKey, long counter)
	{
		// if we have no key in the message, we use the running number of the counter instead
		if(recordKey!=null)
		{
			return recordKey;
		}
		else
		{
			return "record_" + counter;
		}
	}
	
		
	/**
	 * method sends a message to the relevant kafka target topic.
	 * 
	 * the message will contain the original fields from the source topic, plus the refercence fields 
	 * that are defined in the ruleengine project file that are not already defined in the message itself.
	 * 
	 * For example if the project file contains a reference field "country" which is which is set by an action
	 * in one of the rulegroups and that field is not already existing in the source topic message, then this
	 * field will be added to the message of the target topic.
	 * 
	 * @param kafkaProducer		the kafka producer to use to send the message
	 * @param recordKey			the key of the kafka message
	 * @param message			the message as received from the source topic
	 */
	private void sendTargetTopicMessage(KafkaProducer<String, String> kafkaProducer, String recordKey, JSONObject message)
	{
		// send the resulting data to the target topic
		kafkaProducer.send(new ProducerRecord<String, String>(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET),recordKey, message.toString()));
	}

	/**
	 * method sends a message for those records that failed the ruleengine logik to the relevant kafka target topic.
	 * 
	 * the message will contain the original fields from the source topic, plus the refercence fields 
	 * that are defined in the ruleengine project file that are not already defined in the message itself.
	 * 
	 * For example if the project file contains a reference field "country" which is which is set by an action
	 * in one of the rulegroups and that field is not already existing in the source topic message, then this
	 * field will be added to the message of the target topic.
	 * 
	 * @param kafkaProducerFailed		the kafka producer to use to send the message
	 * @param recordKey					the key of the kafka message
	 * @param message					the message as received from the source topic
	 */
	private void sendFailedTargetTopicMessage(KafkaProducer<String, String> kafkaProducerFailed, String recordKey, JSONObject message)
	{
		// send the resulting data to the target topic
		kafkaProducerFailed.send(new ProducerRecord<String, String>(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED),recordKey, message.toString()));
	}

	/**
	 * method adds the relevant results from the ruleengine execution to the JSON formatted object (message) and
	 * submits the message to the kafka topic for logging
	 * 
	 * for each input message from the source topic and for each rule defined in the ruleengine project file
	 * one message is created. So if there are 10 rules defined, then this method generates ten messages - one
	 * for each rule.
	 * 
	 * there are several fields added to the output message which are details of the execution of the ruleengine and rules.
	 * E.g. if the group, subgroup or rule failed, the rule message and the logical operators of the subgroups and rules.
	 * 
	 * @param kafkaProducer		producer used for the logging topic
	 * @param recordKey			key of the kafka source message
	 * @param message			the kafka source message
	 * @param ruleEngine		reference to the ruleengine instance
	 */
	private void sendLoggingTargetTopicMessage(KafkaProducer<String, String> kafkaProducerLogging, String recordKey, JSONObject message, BusinessRulesEngine ruleEngine)
	{
		// loop over all rule groups
		for(int f=0;f<ruleEngine.getGroups().size();f++)
        {
        	RuleGroup group = ruleEngine.getGroups().get(f);
        	// loop over all subgroups
    		for(int g=0;g<group.getSubGroups().size();g++)
            {
        		RuleSubGroup subgroup = group.getSubGroups().get(g);
        		// get the ruleengine execution results of the subgroup
        		ArrayList <RuleExecutionResult> results = subgroup.getExecutionCollection().getResults();
        		// loop over all results
        		for (int h= 0;h< results.size();h++)
                {
        			RuleExecutionResult result = results.get(h);
        			XmlRule rule = result.getRule();
        			
    				String groupId  = group.getId();
    				long groupFailed = (long)group.getFailed();
    				String subgroupId = subgroup.getId();
    				long subgroupFailed = (long)subgroup.getFailed();
    				String subGroupOperator = subgroup.getLogicalOperatorSubGroupAsString();
    				String rulesOperator = subgroup.getLogicalOperatorRulesAsString();
    				String ruleId = rule.getId();
    				long ruleFailed = (long)rule.getFailed();
    				String ruleMessage = result.getMessage();
    				
    				// add ruleengine fields to the message
    				message.put(Constants.RULEENGINE_FIELD_GROUP_ID, groupId);
    				message.put(Constants.RULEENGINE_FIELD_GROUP_FAILED, groupFailed);
    				message.put(Constants.RULEENGINE_FIELD_SUBGROUP_ID, subgroupId);
    				message.put(Constants.RULEENGINE_FIELD_SUBGROUP_FAILED, subgroupFailed);
    				message.put(Constants.RULEENGINE_FIELD_SUBGROUP_OPERATOR, subGroupOperator);
    				message.put(Constants.RULEENGINE_FIELD_RULES_OPERATOR, rulesOperator);
    				message.put(Constants.RULEENGINE_FIELD_RULE_ID, ruleId);
    				message.put(Constants.RULEENGINE_FIELD_RULE_FAILED, ruleFailed);
    				message.put(Constants.RULEENGINE_FIELD_RULE_MESSAGE, ruleMessage);
    				
    				kafkaProducerLogging.send(new ProducerRecord<String, String>(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING),recordKey, message.toString()));
                }
            }
        }
	}
	
	/**
	 * in the ruleengine project file there may be additional fields defined that are used
	 * by the ruleengine -which are not available in the input message. For example fields
	 * that are used by actions to update the data.
	 * 
	 * these fields will be added to the rowfield collection and subsequently also to the output
	 * to the target topic.
	 * 
	 * 
	 * @param collection	collection of row fields
	 */
	private void addReferenceFields(RowFieldCollection collection)
	{
		// only evaluate the reference fields on the first message
		if(!referenceFieldsCollected)
		{
			ArrayList <ReferenceField>referenceFields = ruleEngine.getReferenceFields();
			for(int i=0;i<referenceFields.size();i++)
			{
				ReferenceField referenceField = referenceFields.get(i);
				boolean existField = collection.existField(referenceField.getName());
				if(!existField)
				{
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName()));
					collection.addField(referenceField.getName(),null);
				}
			}
			referenceFieldsCollected = true;
		}
		// once the reference fields to be added have been determined we do not loop over the collection again.
		// so we are caching the fields but:
		// note, that we need to set all fields to null as they are updated from the business rule engine
		else
		{
			for(int i=0;i<ruleEngineProjectFileReferenceFields.size();i++)
			{
				RowField field = ruleEngineProjectFileReferenceFields.get(i);
				field.setValue(null);
				collection.addField(field);
			}
		}
	}
	
	/**
	 * Prepare the configuration for the kafka consumer
	 * 
	 * @param bootstrapServers	the kafka server(s) to use
	 * @param groupId			the kafka id of the group
	 * @return					properties for instantiating consumer
	 */
	private static Properties getConsumerProperties(String bootstrapServers, String groupId) 
	{
		// prepare properties to initialize kafka consumer
		Properties properties = new Properties();
		properties.put("bootstrap.servers", bootstrapServers);
		properties.put("group.id", groupId);
		properties.put("enable.auto.commit", enableAutoCommit);
		properties.put("auto.commit.interval.ms", autoCommitIntervalMs);
		properties.put("key.deserializer", keyDeserializer);
		properties.put("value.deserializer", valueDeserializer);
		properties.put("auto.offset.reset", autoOffsetReset);
 
        return properties;
    }

	/**
	 * Prepare the configuration for the kafka producer
	 * 
	 * @param bootStrapServers	the kafka brokers(s) used
	 * @return					properties with producer details
	 */
	private static Properties getProducerProperties(String bootStrapServers) 
	{
		Properties properties = new Properties();
		properties.put("bootstrap.servers", bootStrapServers);
		properties.put("acks", acks);
		properties.put("retries", retries);
		properties.put("batch.size", batchSize);
		properties.put("linger.ms", lingerMs);
		properties.put("buffer.memory", bufferMemory);
		properties.put("key.serializer", keySerializer);
		properties.put("value.serializer", valueSerializer);
        
        return properties;
	}
	
	/**
	 * 
	 * @return	the current date time in a standard format
	 */
	private String getExecutionDateTime()
	{
		return sdf.format(new Date());
	}
	
	/**
	 * 
	 * @param type	type of the message
	 * @param text	text of the message
	 * @return		standardized text to output
	 */
	private String getSystemMessage(String type, String text)
	{
		return "[" + getExecutionDateTime() + "] " + type + " " + text;
	}
}
