/*
 * @(#)WordCountMapper.java $version 2014. 1. 8.
 *
 * Copyright 2007 NHN Corp. All rights Reserved.
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author NHN Technology services. Kwanil Lee.
 */
public class WordCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
	@Override
	public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		while (stringTokenizer.hasMoreTokens()) {
			Text text = new Text(stringTokenizer.nextToken());
			output.collect(text, new IntWritable(1));
		}
	}
}
