/*
 * @(#)WordCount.java $version 2014. 1. 8.
 *
 * Copyright 2007 NHN Corp. All rights Reserved.
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author NHN Technology services. Kwanil Lee.
 */
public class WordCount {
	public static void main(String[] args) throws Exception {
		JobConf jobConf = new JobConf(WordCount.class);
		jobConf.setJobName("WordCount");

		// 윈도우에서만 돌아갈수 있도록 세팅 리눅스에선 지워서 나가야합니다.
		jobConf.set("fs.file.impl", "common.WinLocalFileSystem");

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		jobConf.setMapperClass(WordCountMapper.class);
		jobConf.setCombinerClass(WordCountReducer.class);
		jobConf.setReducerClass(WordCountReducer.class);

		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		JobClient.runJob(jobConf);
	}
}
