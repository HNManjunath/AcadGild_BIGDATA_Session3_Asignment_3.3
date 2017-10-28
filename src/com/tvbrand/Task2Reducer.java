package com.tvbrand;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer Class two
public class Task2Reducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Hash Map to store the values during processing
		HashMap<String, Integer> data = new HashMap<String, Integer>();

		for (Text value : values) {
			String[] companyName = value.toString().split("\\|");

			if (companyName[0].equalsIgnoreCase("Onida")) {		//fetching company name at index 0th position
				if (data.containsKey(companyName[3])) {			//getting state value
					data.put(companyName[3], data.get(companyName[3]) + 1);
				} else {
					data.put(companyName[3], 1);
				}
			}
		}

		// To display the processed output
		for (Entry<String, Integer> finalData : data.entrySet()) {
			context.write(new Text(finalData.getKey()), new Text(finalData.getValue().toString()));
		}
	}
}