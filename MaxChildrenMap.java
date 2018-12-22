package com;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxChildrenMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable keyin, Text valuein, Context context) throws IOException, InterruptedException {

		String temp = valuein.toString();
		String[] arr = temp.split(",");
		String keyout = arr[3];
		context.write(new Text(keyout), valuein);

	}
}

/*
 * Mapper s&s Reducer key out 1994,1,1 12311 1, 1994,1,1,sunday,12311
 * 1994,1,2,monday,12312
 * 
 * 
 */
