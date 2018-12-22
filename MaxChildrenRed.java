package com;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxChildrenRed extends Reducer<Text, Text, Text, LongWritable> {

	@Override
	protected void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*
		 * 1 1994,1,1,Saturday,8096 1994,1,2,Sunday,7772 1994,1,3,Monday,10142
		 * 1994,1,4,Tuesday,11248
		 */

		String keyOut = "";
		Long max = (long) 0;
		for (Text val : values) {

			String temp = val.toString();
			String[] arr = temp.split(",");
			Long noOfChildren = new Long(arr[4]);
			// 10142 < 10142
			if (max < noOfChildren) {
				max = noOfChildren;
				keyOut = arr[0] + "," + arr[1] + "," + arr[2]+","+arr[3];
			}
		}

		context.write(new Text(keyOut), new LongWritable(max));
	}
}
//1994,1,1,sunday	2131