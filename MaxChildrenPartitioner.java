package com;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxChildrenPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text keyin, Text valin, int noOfReducer) {

		String dayOfWeek = keyin.toString();
		if (dayOfWeek.equals("Sunday")) {
			return 0;
		} else if (dayOfWeek.equals("Monday")) {
			return 1;
		} else if (dayOfWeek.equals("Tuesday")) {
			return 2;
		} else if (dayOfWeek.equals("Wednesday")) {
			return 3;
		} else if (dayOfWeek.equals("Thursday")) {
			return 4;
		} else if (dayOfWeek.equals("Friday")) {
			return 5;
		}
		return 6;
	}

}
