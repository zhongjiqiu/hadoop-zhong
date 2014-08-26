package org.zkpk.day0821;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class productNum extends Configured implements Tool {
	static class productMapper extends Mapper<Object,Text,Text,IntWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr=value.toString().split(",",-1);
			context.write(new Text(arr[6]+"*"+arr[7]), new IntWritable(1));
		}
		
	}
	static class productReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int sum=0;
			
			for(IntWritable value:values){
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job1=new Job(new Configuration());
		job1.setJarByClass(productNum.class);
		job1.setMapperClass(productMapper.class);
		job1.setReducerClass(productReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.1.100:9000/out/11"));
		job1.waitForCompletion(true);
		return job1.waitForCompletion(true)?0:1;
		
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new productNum(), args);
		System.exit(exitCode);
	}

}
