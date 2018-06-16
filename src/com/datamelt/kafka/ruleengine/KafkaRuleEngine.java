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
import java.util.Date;
import java.util.Properties;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import com.datamelt.rules.core.ReferenceField;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.Constants;
import com.datamelt.util.PropertiesFileException;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;

public class KafkaRuleEngine
{
	private static ArrayList<RowField> ruleEngineProjectFileReferenceFields  = new ArrayList<RowField>();
	private static Properties properties 									 = new Properties();
	private static Properties kafkaConsumerProperties						 = new Properties();
	private static Properties kafkaProducerProperties						 = new Properties();
	private static SimpleDateFormat sdf 							 		 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static boolean outputToFailedTopic								 = false;
	private static int failedMode										 	 = 0;
	private static int failedNumberOfGroups							 	 	 = 0;
	private static long kafkaConsumerPoll									 = 100;
	
	public static void main(String[] args) throws Exception
	{
		if(args.length==0 || args.length<4)
    	{
    		help();
    	}
		else
		{
			// load kafka ruleengine properties file
			properties = loadProperties(args[0]);
			
			// load kafka consumer properties file
			kafkaConsumerProperties = loadProperties(args[1]);
			
			// add properties from the kafka ruleengine properties file
			kafkaConsumerProperties.put(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS, getProperty(Constants.PROPERTY_KAFKA_BROKERS));
			kafkaConsumerProperties.put(Constants.PROPERTY_KAFKA_CONSUMER_GROUP_ID, getProperty(Constants.PROPERTY_KAFKA_GROUP_ID));
			
			// load kafka producer properties file
			kafkaProducerProperties = loadProperties(args[2]);
			
			// add properties from the kafka ruleengine properties file
			kafkaProducerProperties.put(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS, getProperty(Constants.PROPERTY_KAFKA_BROKERS));
			
			// process properties into variables;
			processProperties();

			// get a reference to the ruleengine project zip file containing the business rules
			ZipFile ruleengineProjectZipFile = getRuleEngineProjectZipFile(args[3]);
			
			System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka brokers: " + getProperty(Constants.PROPERTY_KAFKA_BROKERS)));
			System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka source topic: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)));
			System.out.println(getSystemMessage(Constants.LEVEL_INFO,"ruleengine project file: " + args[3]));
			System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka target topic: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)));
			System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka target topic failed: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)));
			System.out.println(getSystemMessage(Constants.LEVEL_INFO,"kafka logging topic: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)));
			System.out.println(getSystemMessage(Constants.LEVEL_INFO,""));
			
			// create a RuleEngineConsumerProducer instance and run it
			try
			{
				RuleEngineConsumerProducer ruleEngineConsumerProducer = new RuleEngineConsumerProducer(new BusinessRulesEngine(ruleengineProjectZipFile),kafkaConsumerProperties,kafkaProducerProperties);
				
				ruleEngineConsumerProducer.setKafkaTopicSource(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE));
				ruleEngineConsumerProducer.setKafkaTopicTarget(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET));
				ruleEngineConsumerProducer.setKafkaTopicFailed(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED));
				ruleEngineConsumerProducer.setKafkaTopicLogging(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING));

				ruleEngineConsumerProducer.setFailedMode(failedMode);
				ruleEngineConsumerProducer.setFailedNumberOfGroups(failedNumberOfGroups);
				ruleEngineConsumerProducer.setKafkaConsumerPoll(kafkaConsumerPoll);
				ruleEngineConsumerProducer.setOutputToFailedTopic(outputToFailedTopic);
				
				// we do not want to preserve the detailed results of the ruleengine execution
				// if we are logging the detailed results to a topic
				if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).equals(""))
				{
					ruleEngineConsumerProducer.setPreserveRuleExecutionResults(false);
				}
				
				if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).equals(""))
				{
					ruleEngineConsumerProducer.setKafkaTopicSourceFormat(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT));
				}
				
				if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS).equals(""))
				{
					ruleEngineConsumerProducer.setKafkaTopicSourceFormatCsvFields(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_FIELDS));
				}
				
				if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR).equals(""))
				{
					ruleEngineConsumerProducer.setKafkaTopicSourceFormatCsvSeparator(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT_CSV_SEPARATOR));
				}
				
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
    	System.out.println("The Apache Kafka source topic messages must be in JSON or CSV format. Output will be in JSON format");
    	System.out.println();
    	System.out.println("Four files must be specified, defining various properties for the program and the ruleengine project zip file.");
    	System.out.println();
    	System.out.println("RuleEngineConsumerProducer [properties file] [kafka consumer properties file] [kafka producer properties file]");
    	System.out.println("where [properties file]                : required. path and name of the properties file");
    	System.out.println("      [kafka consumer properties file] : required. path and name of the kafka consumer properties file");
    	System.out.println("      [kafka producer properties file] : required. path and name of the kafka producer properties file");
    	System.out.println("      [rule engine project file]       : required. path and name of the rule engine project file");
    	System.out.println();
    	System.out.println("example: RuleEngineConsumerProducer /home/test/kafka_ruleengine.properties /home/test/kafka_consumer.properties /home/test/kafka_producer.properties /home/test/my_project_file.zip");
    	System.out.println();
    	System.out.println("published as open source under the Apache License. read the licence notice");
    	System.out.println("all code by uwe geercken, 2006-2018. uwe.geercken@web.de");
    	System.out.println();
	}
	
	private static Properties loadProperties(String propertiesFilename) throws PropertiesFileException,FileNotFoundException,IOException
    {
    	Properties properties = new Properties();
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
    	return properties;
    }
	
	private static ZipFile getRuleEngineProjectZipFile(String filename) throws FileNotFoundException,IOException,ZipException
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
	 * in the ruleengine project file there may be additional fields defined that are used
	 * by the ruleengine - which are not available in the input message. For example fields
	 * that are used by actions to update the data.
	 * 
	 * these fields will be added to the rowfield collection and subsequently also to the output
	 * to the target topic.
	 * 
	 * 
	 * @param collection	collection of row fields
	 */
	public static void addReferenceFields(ArrayList <ReferenceField>referenceFields, RowFieldCollection collection)
	{
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
	}
	
	/**
	 * returns a property by specifying the key of the property
	 * 
	 * @param key		the key of a property
	 * @return			the value of the property for the given key
	 */
	private static String getProperty(String key)
	{
		return properties.getProperty(key);
	}
	
	private static void processProperties()
	{
		// get the number of messages the consumer will poll in one go from kafka
		if(getProperty(Constants.PROPERTY_KAFKA_CONSUMER_POLL)!=null && !getProperty(Constants.PROPERTY_KAFKA_CONSUMER_POLL).equals(""))
		{
			try
			{
				kafkaConsumerPoll = Long.parseLong(getProperty(Constants.PROPERTY_KAFKA_CONSUMER_POLL));
			}
			catch(Exception ex)
			{
			}
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

	}
	
	/**
	 * 
	 * @return	the current date time in a standard format
	 */
	private static String getExecutionDateTime()
	{
		return sdf.format(new Date());
	}
	
	/**
	 * 
	 * @param type	type of the message
	 * @param text	text of the message
	 * @return		standardized text to output
	 */
	public static String getSystemMessage(String type, String text)
	{
		return "[" + getExecutionDateTime() + "] " + type + " " + text;
	}
	
	/**
	 * Returns the label used by the rulengine for each record.
	 * 
	 * @param recordKey		key of the kafka message
	 * @param counter		the current counter for the number of messages retrieved
	 * @return
	 */
	public static String getLabel(String recordKey, long counter)
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
}
