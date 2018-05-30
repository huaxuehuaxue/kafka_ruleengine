package com.datamelt.util;

import org.json.JSONObject;

/**
 * Formats RowFieldCollection to JSON or vice versa.
 * 
 * 
 * @author uwe.geercken@web.de - 2018-05-30
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
	 * 
	 * @param collection	rowfield collection
	 * @return				JSONObject containing json formated key/value pairs
	 * @throws Exception	in case a field could not be retrieved from the collection
	 */

	public static JSONObject convertToJson(RowFieldCollection rowFieldCollection) throws Exception
	{
		JSONObject jsonObject = new JSONObject();
		for(int i=0;i<rowFieldCollection.getNumberOfFields();i++)
		{
			RowField field = rowFieldCollection.getField(i);
			if(field.getValue()==null)
			{
				jsonObject.put(field.getName(), JSONObject.NULL);
			}
			else
			{
				jsonObject.put(field.getName(), field.getValue());
			}
		}
		return jsonObject;
	}
	
	
}
