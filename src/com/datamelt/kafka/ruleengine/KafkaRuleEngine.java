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
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;

import com.datamelt.rules.core.ReferenceField;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.Constants;
import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;

public class KafkaRuleEngine
{
	private static ArrayList<RowField> ruleEngineProjectFileReferenceFields  = new ArrayList<RowField>();
	private static Properties properties 									 = new Properties();
	private static String propertiesFilename;
	private static Properties adminClientProperties							 = new Properties();
	private static Properties kafkaConsumerProperties						 = new Properties();
	private static Properties kafkaProducerProperties						 = new Properties();
	private static SimpleDateFormat sdf 							 		 = new SimpleDateFormat(Constants.DATETIME_FORMAT);
	private static boolean outputToFailedTopic								 = false;
	private static int failedMode										 	 = 0;
	private static int failedNumberOfGroups							 	 	 = 0;
	private static long kafkaConsumerPoll									 = 100;
	private static ArrayList<String> excludedFields							 = new ArrayList<>();
	
	private static int logLevel 											 = Constants.LOG_LEVEL_INFO;
	
	public static void main(String[] args) throws Exception
	{
		if(args.length==0 || args.length<5)
    	{
    		help();
    	}
		else
		{
			// if the user specified a log level
			if(args.length==5 && args[4]!=null)
			{
				logLevel = Integer.parseInt(args[4]);
			}
			
			// load kafka ruleengine properties file
			propertiesFilename = args[0];
			properties = loadProperties(propertiesFilename);
			
			// load kafka consumer properties file
			kafkaConsumerProperties = loadProperties(args[1]);
			
			// add properties from the kafka ruleengine properties file
			kafkaConsumerProperties.put(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS, getProperty(Constants.PROPERTY_KAFKA_BROKERS));
			kafkaConsumerProperties.put(Constants.PROPERTY_KAFKA_CONSUMER_GROUP_ID, getProperty(Constants.PROPERTY_KAFKA_GROUP_ID));
			
			// load kafka producer properties file
			kafkaProducerProperties = loadProperties(args[2]);
			
			// add properties from the kafka ruleengine properties file
			kafkaProducerProperties.put(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS, getProperty(Constants.PROPERTY_KAFKA_BROKERS));
			
			// add properties from the kafka ruleengine properties file
			adminClientProperties.put(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS, getProperty(Constants.PROPERTY_KAFKA_BROKERS));
			
			// process properties into variables;
			processProperties();
			
			// check the properties file for availability of variables;
			boolean propertiesFileOk = checkProperties();

			String targetTopic = "[undefined]";
			if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).trim().equals(""))
			{
				targetTopic = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET);
			}
			String failedTopic = "[undefined]";
			if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim().equals(""))
			{
				failedTopic = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED);
			}
			
			String loggingTopic = "[undefined]";
			if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim().equals(""))
			{
				loggingTopic = getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING);
			}
			
			String ruleEngineVersion = "[" + BusinessRulesEngine.getVersion() + "] - last update: [" + BusinessRulesEngine.getLastUpdateDate() + "]";

			log(Constants.LOG_LEVEL_ALL, "Start of KafkaRuleEngine program...");
			log(Constants.LOG_LEVEL_INFO, "JaRE ruleengine version: " + ruleEngineVersion);
			log(Constants.LOG_LEVEL_DETAILED,"properties for the ruleengine => " + properties.toString());
			log(Constants.LOG_LEVEL_DETAILED,"properties for kafka consumer => " + kafkaConsumerProperties.toString());
			log(Constants.LOG_LEVEL_DETAILED,"properties for kafka producer => " + kafkaProducerProperties.toString());
			log(Constants.LOG_LEVEL_DETAILED,"properties for admin client => " + adminClientProperties.toString());
			log(Constants.LOG_LEVEL_ALL, "kafka brokers: " + getProperty(Constants.PROPERTY_KAFKA_BROKERS));
			log(Constants.LOG_LEVEL_ALL, "kafka source topic: " + getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE));
			log(Constants.LOG_LEVEL_ALL, "ruleengine project file: " + args[3]);
			log(Constants.LOG_LEVEL_ALL, "ruleengine project file check interval (seconds): " + getProperty(Constants.PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL));
			log(Constants.LOG_LEVEL_ALL, "kafka target topic: " + targetTopic);
			log(Constants.LOG_LEVEL_ALL, "kafka failed topic: " + failedTopic);
			log(Constants.LOG_LEVEL_ALL, "kafka logging topic: " + loggingTopic);
			log(Constants.LOG_LEVEL_ALL, "");
			
			// check if the zip file is present and accessible and if the ruleengine properties file is ok as well
			boolean zipFileOk = ruleEngineProjectZipFileOk(args[3]); 
			if(zipFileOk && propertiesFileOk)
			{
				// check if we can get a list of topics from the brokers using the AdminClient
				// if not, then the brokers are probably not available
				//boolean kafkaBrokersAvailable = brokersAvailable();
				boolean kafkaBrokersAvailable = true;
				if(kafkaBrokersAvailable)
				{
					
					// create a RuleEngineConsumerProducer instance and run it
					try
					{
						RuleEngineConsumerProducer ruleEngineConsumerProducer = new RuleEngineConsumerProducer(args[3],kafkaConsumerProperties,kafkaProducerProperties);
						
						ruleEngineConsumerProducer.setKafkaTopicSource(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE));
						ruleEngineConsumerProducer.setKafkaTopicTarget(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET));
						ruleEngineConsumerProducer.setKafkaTopicFailed(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED));
						ruleEngineConsumerProducer.setKafkaTopicLogging(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING));
						ruleEngineConsumerProducer.setRuleEngineZipFileCheckModifiedInterval(Integer.parseInt(getProperty(Constants.PROPERTY_RULEENGINE_ZIP_FILE_CHECK_INTERVAL)));
						
						ruleEngineConsumerProducer.setFailedMode(failedMode);
						ruleEngineConsumerProducer.setFailedNumberOfGroups(failedNumberOfGroups);
						ruleEngineConsumerProducer.setKafkaConsumerPoll(kafkaConsumerPoll);
						ruleEngineConsumerProducer.setOutputToFailedTopic(outputToFailedTopic);
						
						// we do not want to preserve the detailed results of the ruleengine execution
						// if we are not logging the detailed results to a topic
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
				else
				{
					String kafkaBrokers = kafkaConsumerProperties.getProperty(Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS);
					KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR,"could not connect to Kafka broker(s) at: " + kafkaBrokers +" in " + Constants.ADMIN_CLIENT_TIMEOUT_MS + " milliseconds");
					KafkaRuleEngine.log(Constants.LOG_LEVEL_ALL,"end of program");
				}
			}
			else
			{
				if(zipFileOk==false)
				{
					KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR,"there was an error with the rule engine project zip file: [" + args[3] + "]");
				}
				if(propertiesFileOk==false)
				{
					KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR,"there was an error with the ruleengine properties file: [" + propertiesFilename + "]");
				}
				KafkaRuleEngine.log(Constants.LOG_LEVEL_ALL,"end of program");
			}
		}
	}

	/**
	 * method to output log messages
	 * 
	 * the default logging level is INFO.  
	 * 
	 */
	public static void log(int messageLogLevel, String message)
	{
		if(messageLogLevel<= logLevel)
		{
			System.out.println(getSystemMessage(Constants.LOG_LEVEL_NAMES[messageLogLevel],message));
		}
	}
	
	private static void help()
	{
		System.out.println("KafkaRuleEngine. Program to process data from an Apache Kafka source topic,");
    	System.out.println("run the business rules from a ruleengine project file against the data and output the");
    	System.out.println("results to an Apache Kafka target topic. Failed rows of data may be output to a different topic.");
    	System.out.println();
    	System.out.println("Additionally an optional topic for logging may be specified which will contain the detailed");
    	System.out.println("results of the execution of the individual rules.");
    	System.out.println();
    	System.out.println("The Apache Kafka source topic messages must be in JSON or CSV format. Output will be in JSON format");
    	System.out.println();
    	System.out.println("Four files must be specified, defining various properties for the program and the ruleengine project zip file.");
    	System.out.println();
    	System.out.println("KafkaRuleEngine [ruleengine properties file] [kafka consumer properties file] [kafka producer properties file] [log level]");
    	System.out.println("where [ruleengine properties file]     : required. path and name of the ruleengine properties file");
    	System.out.println("      [kafka consumer properties file] : required. path and name of the kafka consumer properties file");
    	System.out.println("      [kafka producer properties file] : required. path and name of the kafka producer properties file");
    	System.out.println("      [rule engine project file]       : required. path and name of the rule engine project file");
    	System.out.println("      [log level]                      : optional. supresses messages above the selected logging level. 1=Error, 2=Warnings, 3=Info, 4=Detailed");
    	System.out.println();
    	System.out.println("example: KafkaRuleEngine /home/test/kafka_ruleengine.properties /home/test/kafka_consumer.properties /home/test/kafka_producer.properties /home/test/my_project_file.zip 3");
    	System.out.println();
    	System.out.println("published as open source under the Apache License. read the licence notice");
    	System.out.println("check https://github.com/uwegeercken for source code, documentation and samples.");
    	System.out.println("all code by uwe geercken, 2006-2018. uwe.geercken@web.de");
    	System.out.println();
	}
	

	/**
	 * Loads the properties from the given file
	 * 
	 * @param filename		the path and name of the properties file
	 * @return				Properties object
	 */
	private static Properties loadProperties(String propertiesFilename) 
    {
    	Properties properties = new Properties();
		File propertiesFile = new File(propertiesFilename);
    	if(!propertiesFile.exists())
    	{
    		log(Constants.LOG_LEVEL_ERROR,"properties file not found: [" + propertiesFilename + "]");
    	}
    	else if(!propertiesFile.canRead())
    	{
    		log(Constants.LOG_LEVEL_ERROR,"properties file can not be read: [" + propertiesFilename + "]");
    	}
    	else if(!propertiesFile.isFile())
    	{
    		log(Constants.LOG_LEVEL_ERROR,"properties file is not a file: [" + propertiesFilename + "]");
    	}
    	else
    	{
    		try(FileInputStream inputStream = new FileInputStream(propertiesFile);)
    		{
    			properties.load(inputStream);
    			inputStream.close();
    		}
    		catch(Exception ex)
    		{
    			log(Constants.LOG_LEVEL_ERROR,"properties file not found: [" + propertiesFilename + "]");
    		}
    	}
    	return properties;
    }
	
	/**
	 * checks if the ruleengine project zip file is accessible
	 * 
	 * @param filename		the path and name of the project zip file
	 * @return				boolean indicator if file is accessible
	 */
	private static boolean ruleEngineProjectZipFileOk(String filename) 
    {
    	boolean ruleEngineProjectZipFileOk = true;
		File ruleengineFile = new File(filename);
		
    	if(!ruleengineFile.exists())
    	{
    		log(Constants.LOG_LEVEL_ERROR,"ruleengine project zip file not found: [" + filename + "]");
    		ruleEngineProjectZipFileOk = false;
    	}
    	else if(!ruleengineFile.canRead())
    	{
    		log(Constants.LOG_LEVEL_ERROR,"ruleengine project zip file can not be read: [" + filename + "]");
    		ruleEngineProjectZipFileOk = false;
    	}
    	else if(!ruleengineFile.isFile())
    	{
    		log(Constants.LOG_LEVEL_ERROR,"ruleengine project zip file is not a file: [" + filename + "]");
    		ruleEngineProjectZipFileOk = false;
    	}
   		return ruleEngineProjectZipFileOk;
    }
	
	/**
	 * checks is the specified broker(s) is (are) available
	 * 
	 * A check using the AdminClient is made to retrieve a list of topics. If this fails
	 * then it is assumed that the broker(s) is (are) not available
	 * 
	 * @return		boolean indicator is broker(s) is (are) available
	 */
	private static boolean brokersAvailable()
	{
		try (AdminClient client = AdminClient.create(adminClientProperties)) 
		{
			client.listTopics(new ListTopicsOptions().timeoutMs(Constants.ADMIN_CLIENT_TIMEOUT_MS)).listings().get();
			return true;
		}
		catch(ExecutionException cex)
		{
			log(Constants.LOG_LEVEL_ERROR,cex.getMessage());
			return false;
		}
		catch (Exception ex)
		{
			log(Constants.LOG_LEVEL_ERROR,ex.getMessage());
			return false;
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
	 * these additional fields are initialized to the following default values:
	 *  - String to an empty string
	 *  - Date to null
	 *  - integer to zero
	 *  - long to zero
	 *  - float to zero
	 *  - double to zero
	 *  - boolean to false
	 *  - BigDecimal to zero
	 *  
	 * @param referenceFields	ArrayList of reference fields
	 * @param collection		collection of row fields
	 */
	public static void addReferenceFields(ArrayList <ReferenceField>referenceFields, RowFieldCollection collection)
	{
		for(int i=0;i<referenceFields.size();i++)
		{
			ReferenceField referenceField = referenceFields.get(i);
			boolean existField = collection.existField(referenceField.getName());
			if(!existField)
			{
				if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_STRING)
				{
					String value = "";
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_INTEGER)
				{
					int value=0;
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_LONG)
				{
					long value=0;
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_DOUBLE)
				{
					double value=0.0d;
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_FLOAT)
				{
					float value=0.0f;
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_BOOLEAN)
				{
					boolean value=false;
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_BIGDECIMAL)
				{
					BigDecimal value=new BigDecimal(0);
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else if(referenceField.getJavaTypeId()==ReferenceField.FIELD_TYPE_ID_DATE)
				{
					Date value=null;
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName(),value));
					collection.addField(referenceField.getName(),value);
				}
				else
				{
					ruleEngineProjectFileReferenceFields.add(new RowField(referenceField.getName()));
					collection.addField(referenceField.getName(),null);
				}
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
				log(Constants.LOG_LEVEL_ERROR,"error converting property to long value [ " + Constants.PROPERTY_KAFKA_CONSUMER_POLL + "] from properties file [" + propertiesFilename + "]");
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
				log(Constants.LOG_LEVEL_ERROR,"error converting property to integer value [ " + Constants.PROPERTY_RULEENGINE_FAILED_MODE + "] from properties file [" + propertiesFilename + "]");
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
					log(Constants.LOG_LEVEL_ERROR,"error converting property to integer value [ " + Constants.PROPERTY_RULEENGINE_FAILED_NUMBER_OF_GROUPS + "] from properties file [" + propertiesFilename + "]");
				}
			}
		}

		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).equals(""))
		{
			outputToFailedTopic = true;
		}
		
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS).equals(""))
		{
			String[] fields = getProperty(Constants.PROPERTY_KAFKA_TOPIC_EXCLUDE_FIELDS).split(Constants.PROPERTY_VALUES_SEPARATOR);
			excludedFields = new ArrayList<String>(Arrays.asList(fields));
		}
	}
	
	private static boolean checkProperties()
	{
		boolean propertiesOk = true;
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).equals(""))
		{
			propertiesOk = false;
			log(Constants.LOG_LEVEL_ERROR,Constants.PROPERTY_KAFKA_TOPIC_SOURCE + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT).equals(""))
		{
			propertiesOk = false;
			log(Constants.LOG_LEVEL_ERROR,Constants.PROPERTY_KAFKA_TOPIC_SOURCE_FORMAT + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)==null || getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).equals(""))
		{
			propertiesOk = false;
			log(Constants.LOG_LEVEL_ERROR,Constants.PROPERTY_KAFKA_TOPIC_TARGET + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_BROKERS)==null || getProperty(Constants.PROPERTY_KAFKA_BROKERS).equals(""))
		{
			propertiesOk = false;
			log(Constants.LOG_LEVEL_ERROR,Constants.PROPERTY_KAFKA_BROKERS + " is undefined in properties file [" + propertiesFilename + "]");
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).trim().equals(""))
		{
			if (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET).trim()))
			{
				log(Constants.LOG_LEVEL_ERROR,"source topic can not be the same as the target topic");
				propertiesOk = false;
			}
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim().equals(""))
		{
			if (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_LOGGING).trim()))
			{
				log(Constants.LOG_LEVEL_ERROR,"source topic can not be the same as the logging topic");
				propertiesOk = false;
			}
		}
		if(getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals("") && getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED)!=null && !getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim().equals(""))
		{
			if (getProperty(Constants.PROPERTY_KAFKA_TOPIC_SOURCE).trim().equals(getProperty(Constants.PROPERTY_KAFKA_TOPIC_TARGET_FAILED).trim()))
			{
				log(Constants.LOG_LEVEL_ERROR,"source topic can not be the same as the failed topic");
				propertiesOk = false;
			}
		}

		return propertiesOk;
	}
	
	/**
	 * Get the current data and time in a standard format
	 * 
	 * @return	datetime in standard format
	 */
	private static String getExecutionDateTime()
	{
		return sdf.format(new Date());
	}
	
	/**
	 * Formats messages in a standard way
	 * 
	 * @param type	type of the message (log level)
	 * @param text	text of the message
	 * @return		standardized text to output
	 */
	public static String getSystemMessage(String type, String text)
	{
		return "[" + getExecutionDateTime() + "] " + type + " " + text;
	}
	
	/**
	 * Returns the label used by the ruleengine for each record.
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
	
	/**
	 * A list of fields can be defined in the properties file, which should not
	 * be transfered to the output topics
	 * 
	 * @return 	arrylist of fields to be excluded from the output
	 */
	public static ArrayList<String> getExcludedFields()
	{
		return excludedFields;
	}
}
