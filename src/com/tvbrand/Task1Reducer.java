package com.tvbrand;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text, Text, Text, Text>
{	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		//	Hash Map to store the values during processing
		HashMap<String, Integer> data = new HashMap<String, Integer>();
		
		for (Text value : values) {
			String [] record = value.toString().split("\\|");
			
			if(data.containsKey(record[0])){			//getting company name
				data.put(record[0], 
						data.get(record[0]) + 1);		//if same company then increment the value for that.
			} else {
				data.put(record[0], 1);
			}
		}
		
		for(Entry<String, Integer> finaldata : data.entrySet()) {			//looping for multiple records
			context.write(new Text(finaldata.getKey()), 
					new Text(finaldata.getValue().toString()));
		}
	}
}
