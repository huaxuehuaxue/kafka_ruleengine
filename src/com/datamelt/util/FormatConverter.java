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

import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONObject;

/**
 * Formats RowFieldCollection to JSON or vice versa.
 * 
 * 
 * @author uwe.geercken@web.de - 2018-08-05
 *
 */
public class FormatConverter
{
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param record	a record of data 
	 * @return			a rowfield collection
	 */
	public static RowFieldCollection convertFromJson(String record)
	{
		// create a JSON object from the record
		JSONObject obj = new JSONObject(record);
		
        // create a new rowfield collection using keys and values
		return new RowFieldCollection(obj.toMap());  

	}
	
	/**
	 * creates a JSONObject from the fields available in the rowfield collection.
	 * 
	 * if fields to be excluded from the output are defined, then these will be
	 * excluded here.
	 * 
	 * @param collection	rowfield collection
	 * @return				JSONObject containing json formated key/value pairs
	 * @throws Exception	in case a field could not be retrieved from the collection
	 */

	public static JSONObject convertToJson(RowFieldCollection rowFieldCollection, ArrayList<String> excludedFields) throws Exception
	{
		JSONObject jsonObject = new JSONObject();
		for(int i=0;i<rowFieldCollection.getNumberOfFields();i++)
		{
			RowField field = rowFieldCollection.getField(i);
			// don't add fields that should be excluded
			if(!excludedFields.contains(field.getName()))
			{
				if(field.getValue()==null)
				{
					jsonObject.put(field.getName(), JSONObject.NULL);
				}
				else
				{
					jsonObject.put(field.getName(), field.getValue());
				}
			}
		}
		return jsonObject;
	}
	
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param record			the kafka message value in CSV format 
	 * @param recordFieldNames	the CSV fieldnames as specified in the properties file
	 * @param separator			the separator used between the individual fields in the kafka message
	 * @return					a rowfield collection
	 */
	public static RowFieldCollection convertFromCsv(String record, String recordFieldNames, String separator)
	{
		String fieldNames[] = recordFieldNames.split(Constants.PROPERTY_VALUES_SEPARATOR);
		String fields[] = record.split(separator);
		
        // create a new rowfield collection using keys and values
		return new RowFieldCollection(fieldNames,fields);  

	}
	
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param record			the kafka message value in AVRO format 
	 * @param schema 			the AVRO schema corresponding to the message
	 * @return					a rowfield collection
	 */
	public static RowFieldCollection convertFromAvro(GenericRecord record,Schema schema)
	{
		RowFieldCollection rowFieldCollection = null;
		try
		{
			rowFieldCollection = new RowFieldCollection(record, schema);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
        
		return rowFieldCollection;  

	}
}
