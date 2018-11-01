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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.zip.ZipFile;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import com.datamelt.rules.core.RuleExecutionResult;
import com.datamelt.rules.core.RuleGroup;
import com.datamelt.rules.core.RuleSubGroup;
import com.datamelt.rules.core.XmlRule;
import com.datamelt.rules.engine.BusinessRulesEngine;
import com.datamelt.util.Constants;
import com.datamelt.util.FormatConverter;
import com.datamelt.util.RowFieldCollection;

/**
 * Reads JSON formatted data from a Kafka topic, runs business rules (logic) on the data and
 * outputs the resulting data to a Kafka target topic.
 * 
 * Optionally the detailed results of the execution of the ruleengine may be ouput to a defined
 * topic for logging purposes. In this case the topic will contain one output message for each
 * input message and rule. E.g. if there are 10 rules in the ruleengine project file, then for any
 * received input message, 10 output messages are generated.
 * 
 * The source topic data is expected to be in JSON format. Output will be in JSON format.
 * 
 * @author uwe geercken - 2018-09-13
 *
 */
public class RuleEngineConsumerProducer implements Runnable
{
	private Properties kafkaConsumerProperties						 = new Properties();
	private Properties kafkaProducerProperties						 = new Properties();
	private boolean outputToFailedTopic								 = false; //default
	private int failedMode										 	 = 0; //default
	private int failedNumberOfGroups							 	 = 0; //default
	private long kafkaConsumerPoll									 = 100; //default
	private boolean preserveRuleExecutionResults					 = true; //default
	private String kafkaTopicSourceFormat							 = "json"; //default
	private String kafkaTopicSourceFormatCsvFields;
	private String kafkaTopicSourceFormatCsvSeparator;
	private String kafkaTopicSource;
	private String kafkaTopicTarget;
	private String kafkaTopicLogging;
	private String kafkaTopicFailed;
	private String ruleEngineZipFile;
	private String ruleEngineZipFileWithoutPath;
	private int ruleEngineZipFileCheckModifiedInterval 				= 60; //Default
	
	private WatchService watcher = FileSystems.getDefault().newWatchService();
	private WatchKey key;
	
	private static volatile boolean keepRunning 					 = true;
	
	public RuleEngineConsumerProducer (String ruleEngineZipFile, Properties kafkaConsumerProperties, Properties kafkaProducerProperties) throws Exception
	{
		this.kafkaConsumerProperties = kafkaConsumerProperties;
		this.kafkaProducerProperties = kafkaProducerProperties;
		this.ruleEngineZipFile = ruleEngineZipFile;
		
		Path p = Paths.get(ruleEngineZipFile);
		Path ruleEngineZipFilePath = p.getParent();
		this.ruleEngineZipFileWithoutPath = p.getFileName().toString();
		
		// register watcher for changed or deleted project zip file
		key = ruleEngineZipFilePath.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY,StandardWatchEventKinds.ENTRY_DELETE);
		KafkaRuleEngine.log(Constants.LOG_LEVEL_DETAILED, "registered watcher for changed or deleted project zip file");
		
		KafkaRuleEngine.log(Constants.LOG_LEVEL_DETAILED, "adding shutdown hook");
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
		    public void run() {
	        	KafkaRuleEngine.log(Constants.LOG_LEVEL_INFO,"program shutdown...");
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
	
	/**
	 * runs the RuleEngineConsumerProducer program. Messages are read from an input topic, processed using the ruleengine
	 * and are output to the target topic(s).
	 * 
	 * There is always a minimum of one input topic and one output topic. there can be up to three output topics:
	 * - output of messages of the input topic after the business logic was applied
	 * - output of those messages that failed the business logic to a different topic
	 * - output of the detailed results of the business logic to a different topic
	 */
	public synchronized void run()
	{
		// used for counting the number of messages/records.
		// if no key is defined in the kafka message, then this value is used
		// as the label for each row during the ruleengine execution 
		long counter = 0;
		
		// used for calculating the elapsedtime which is used for reloading 
		// the ruleengine project zip file if it has changed
		long startTime = System.currentTimeMillis();

		File ruleEngineProjectFile = new File(ruleEngineZipFile);
		
		BusinessRulesEngine ruleEngine = null;
		try
		{
			KafkaRuleEngine.log(Constants.LOG_LEVEL_DETAILED, "initializing ruleengine with project zip file: [" + ruleEngineProjectFile + "]");
			// instantiate the ruleengine with the project zip file
			ruleEngine = new BusinessRulesEngine(new ZipFile(ruleEngineProjectFile));

			KafkaRuleEngine.log(Constants.LOG_LEVEL_DETAILED, "preserve ruleengine execution results to: [" + preserveRuleExecutionResults + "]");
			// preserve rule execution results or not
			ruleEngine.setPreserveRuleExcecutionResults(preserveRuleExecutionResults);
		}
		catch(Exception ex)
		{
			KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "error processing the ruleengine project file: [" + ruleEngineProjectFile + "]");
			KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "no data will be consumed from the kafka topic until a valid project file was processed");
			// we do not want to start reading data, if the ruleengine produced an exception
			keepRunning=false;
			
		}
		
		// create consumer and producers for the source topic
		try(
				KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
				KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
				KafkaProducer<String, String> kafkaProducerLogging = new KafkaProducer<>(kafkaProducerProperties);
				KafkaProducer<String, String> kafkaProducerFailed = new KafkaProducer<>(kafkaProducerProperties)
			)
		{
			// subscribe to the given kafka topic
			kafkaConsumer.subscribe(Arrays.asList(kafkaTopicSource));
			
			boolean errorReloadingProjectZipFile = false;
			
			// process while we receive messages and if we haven't encountered an exception
			while (true && keepRunning) 
	        {
				long currentTime = System.currentTimeMillis();
				long elapsedTime = (currentTime - startTime)/1000;

				ConsumerRecords<String, String> records = null;
				
				// if the check modified interval has passed, check if the ruleengine project zip
				// file has changed and if so, reload the file
				if(elapsedTime >= ruleEngineZipFileCheckModifiedInterval)
				{
					startTime = currentTime;
					if(errorReloadingProjectZipFile)
					{
						KafkaRuleEngine.log(Constants.LOG_LEVEL_INFO, "currently no data is processed because of a previous error reloading the project zip file: [" + ruleEngineZipFile + "]");
					}
					KafkaRuleEngine.log(Constants.LOG_LEVEL_DETAILED, "checking if project zip file has changed: [" + ruleEngineZipFile + "]");
					boolean reload = checkFileChanges();
					if(reload)
					{
						KafkaRuleEngine.log(Constants.LOG_LEVEL_DETAILED, "project zip file has changed - reloading: [" + ruleEngineZipFile + "]");
			        	synchronized(ruleEngine)
			        	{
							try
				        	{
				        		// reload the ruleengine project zip file
								ruleEngine.reloadZipFile(new ZipFile(new File(ruleEngineZipFile)));
								errorReloadingProjectZipFile = false;
				        		KafkaRuleEngine.log(Constants.LOG_LEVEL_INFO, "reloaded ruleengine project file: [" + ruleEngineZipFile + "]");
				        	}
				        	catch(Exception ex)
				        	{
				        		errorReloadingProjectZipFile = true;
				        		KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "could not process changed ruleengine project file: [" + ruleEngineZipFile + "]");
				        		KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "no further data will be processed, until a valid ruleengine project zip file is available");
				        	}
			        	}
					}
				}
				
				// only process data if the ruleengine project zip file was correctly processed
				if(!errorReloadingProjectZipFile)
				{
					// poll records from kafka
					records = kafkaConsumer.poll(kafkaConsumerPoll);
					
					// loop over the records/messages
					for (ConsumerRecord<String, String> record : records) 
					{
						counter++;
						try
						{
							// create a collection of fields from the incoming record
							RowFieldCollection collection = null;
							if(kafkaTopicSourceFormat.equals(Constants.MESSAGE_FORMAT_JSON))
							{
								collection = FormatConverter.convertFromJson(record.value());
							}
							else if(kafkaTopicSourceFormat.equals(Constants.MESSAGE_FORMAT_CSV))
							{
								collection = FormatConverter.convertFromCsv(record.value(),kafkaTopicSourceFormatCsvFields,kafkaTopicSourceFormatCsvSeparator);
							}
							
							// add fields that have been additionally defined in the ruleengine project file
							// these are fields that are not present in the message but used by the rule engine
							KafkaRuleEngine.addReferenceFields(ruleEngine.getReferenceFields(), collection);
						
							// label for the ruleengine. used for assigning a unique label to each record
							// this is useful when outputting the detailed results.
							String label = KafkaRuleEngine.getLabel(record.key(), counter);
							
							// run the ruleengine to apply logic and execute actions
							ruleEngine.run(label, collection);
	
							// format the values from the rowfield collection as json for output
							JSONObject jsonMessage = FormatConverter.convertToJson(collection, KafkaRuleEngine.getExcludedFields()); 
							
							// if we don't have a topic specified for the failed messages, then all
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
							}
							
							// clear the collection of details/result
							ruleEngine.getRuleExecutionCollection().clear();

						}
						// if we have a parsing problem with the JSON message, we continue processing
						catch(JSONException jex)
						{
							KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "error parsing message: [" + record.value() + "]");
						}
						// if we have a problem with SSL
						catch (javax.net.ssl.SSLProtocolException sslex)
						{
							KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "server refused certificate or other SSL protocol exception");
							KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, sslex.getMessage());
							keepRunning=false;
						}
						// if we have any other exception 
						catch(Exception ex)
						{
							KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, ex.getMessage());
							keepRunning=false;
						}
					}
				}
			}
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
		kafkaProducer.send(new ProducerRecord<String, String>(kafkaTopicTarget,recordKey, message.toString()));
	}

	/**
	 * method sends a message for those records that failed the ruleengine logic to the relevant kafka target topic.
	 * 
	 * the message will contain the original fields from the source topic, plus the refercence fields 
	 * that are defined in the ruleengine project file that are not already defined in the message itself.
	 * 
	 * For example if the project file contains a reference field "country" which is set by an action
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
		kafkaProducerFailed.send(new ProducerRecord<String, String>(kafkaTopicFailed,recordKey, message.toString()));
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
        		// loop over all results of each subgroup
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
    				
    				kafkaProducerLogging.send(new ProducerRecord<String, String>(kafkaTopicLogging,recordKey, message.toString()));
                }
            }
        }
	}

	public boolean isOutputToFailedTopic()
	{
		return outputToFailedTopic;
	}

	public void setOutputToFailedTopic(boolean outputToFailedTopic)
	{
		this.outputToFailedTopic = outputToFailedTopic;
	}

	public int getFailedMode()
	{
		return failedMode;
	}

	public void setFailedMode(int failedMode)
	{
		this.failedMode = failedMode;
	}

	public int getFailedNumberOfGroups()
	{
		return failedNumberOfGroups;
	}

	public void setFailedNumberOfGroups(int failedNumberOfGroups)
	{
		this.failedNumberOfGroups = failedNumberOfGroups;
	}

	public long getKafkaConsumerPoll()
	{
		return kafkaConsumerPoll;
	}

	public void setKafkaConsumerPoll(long kafkaConsumerPoll)
	{
		this.kafkaConsumerPoll = kafkaConsumerPoll;
	}

	public boolean isPreserveRuleExecutionResults()
	{
		return preserveRuleExecutionResults;
	}

	public void setPreserveRuleExecutionResults(boolean preserveRuleExecutionResults)
	{
		this.preserveRuleExecutionResults = preserveRuleExecutionResults;
	}

	public String getKafkaTopicSource()
	{
		return kafkaTopicSource;
	}

	public void setKafkaTopicSource(String kafkaTopicSource)
	{
		this.kafkaTopicSource = kafkaTopicSource;
	}
	
	public String getKafkaTopicTarget()
	{
		return kafkaTopicTarget;
	}

	public void setKafkaTopicTarget(String kafkaTopicTarget)
	{
		this.kafkaTopicTarget = kafkaTopicTarget;
	}

	public String getKafkaTopicLogging()
	{
		return kafkaTopicLogging;
	}

	public void setKafkaTopicLogging(String kafkaTopicLogging)
	{
		this.kafkaTopicLogging = kafkaTopicLogging;
	}

	public String getKafkaTopicFailed()
	{
		return kafkaTopicFailed;
	}

	public void setKafkaTopicFailed(String kafkaTopicFailed)
	{
		this.kafkaTopicFailed = kafkaTopicFailed;
	}

	public String getKafkaTopicSourceFormat()
	{
		return kafkaTopicSourceFormat;
	}

	public void setKafkaTopicSourceFormat(String kafkaTopicSourceFormat)
	{
		this.kafkaTopicSourceFormat = kafkaTopicSourceFormat;
	}

	public String getKafkaTopicSourceFormatCsvFields()
	{
		return kafkaTopicSourceFormatCsvFields;
	}

	public void setKafkaTopicSourceFormatCsvFields(String kafkaTopicSourceFormatCsvFields)
	{
		this.kafkaTopicSourceFormatCsvFields = kafkaTopicSourceFormatCsvFields;
	}

	public String getKafkaTopicSourceFormatCsvSeparator()
	{
		return kafkaTopicSourceFormatCsvSeparator;
	}

	public void setKafkaTopicSourceFormatCsvSeparator(String kafkaTopicSourceFormatCsvSeparator)
	{
		this.kafkaTopicSourceFormatCsvSeparator = kafkaTopicSourceFormatCsvSeparator;
	}

	/**
	 * method checks if the ruleengine project file has been changed or deleted
	 * 
	 * @return
	 */
	
	private synchronized boolean checkFileChanges()
	{
		boolean fileChanged = false;
		
		// loop over watch events for the given key
		for (WatchEvent<?> event: key.pollEvents())
		{
	        WatchEvent.Kind<?> kind = event.kind();

	        // ignore overflow events
	        if (kind == StandardWatchEventKinds.OVERFLOW) 
	        {
	            continue;
	        }

	        @SuppressWarnings("unchecked")
			WatchEvent<Path> ev = (WatchEvent<Path>)event;

	        // get the filename of the event
	        Path filename = ev.context();
	        String eventFilename = filename.getFileName().toString();

	        if(eventFilename !=null && eventFilename.equals(ruleEngineZipFileWithoutPath))
	        {
		        if(event.kind().equals(StandardWatchEventKinds.ENTRY_DELETE))
		        {
		        	KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "the ruleengine project zip file: [" + filename.getFileName() + "] has been deleted");	        
		        }
		        else
		        {
		        	KafkaRuleEngine.log(Constants.LOG_LEVEL_INFO, "detected changed ruleengine project file: [" + filename.getFileName() + "]");
		        	fileChanged = true;
		        }
	        }
	    }

	    // Reset the key -- this step is critical if you want to receive further watch events.
		// If the key is no longer valid, the directory is inaccessible so exit the loop.
	    try
	    {
	    	key.reset();
	    }
	    catch(Exception ex)
	    {
	    	KafkaRuleEngine.log(Constants.LOG_LEVEL_ERROR, "error resetting the watch file key on folder: [" + ruleEngineZipFileWithoutPath + "]");
	    }
	    return fileChanged;
	}

	public int getRuleEngineZipFileCheckModifiedInterval()
	{
		return ruleEngineZipFileCheckModifiedInterval;
	}

	public void setRuleEngineZipFileCheckModifiedInterval(int ruleEngineZipFileCheckModifiedInterval)
	{
		this.ruleEngineZipFileCheckModifiedInterval = ruleEngineZipFileCheckModifiedInterval;
	}
	
}
