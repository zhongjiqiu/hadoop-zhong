package org.zkpk.day0822;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarketCounts extends Configured implements Tool {
	static class MarketMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr=value.toString().split("\t");
			if(arr.length==6){//判断是否为有效数据
				context.write(new Text(arr[4]), new Text(arr[3]));
			}
		}
		
	}
	static class MarketReducer extends Reducer<Text,Text,Text,IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			HashSet<String> set=new HashSet<String>();
			//使用set集合对相同的value进行去重,set集合无序不重复，而且比map占的内存小
			for(Text value:values){
				set.add(value.toString());
			}			
			context.write(key, new IntWritable(set.size()));
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job2=new Job(new Configuration());
		job2.setJarByClass(MarketCounts.class);
		job2.setMapperClass(MarketMapper.class);
		job2.setReducerClass(MarketReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new MarketCounts(), args);
		System.exit(exitCode);

	}

}
