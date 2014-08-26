package org.zkpk.day0821;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ForecastPrice3 extends Configured implements Tool {
	static class ForecastMapper extends Mapper<Object,Text,Text,DoubleWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[]arr=value.toString().split("\t",-1);
			double avg3=(Double.parseDouble(arr[4])+Double.parseDouble(arr[2])+Double.parseDouble(arr[3]))/3;
			context.write(new Text("黄瓜第五天预测值"), new DoubleWritable(avg3));
		}
		
	}
	static class ForecastReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
				throws IOException, InterruptedException {
			double result=0;
			for(DoubleWritable value:values){
				result+=value.get();
			}
			context.write(key, new DoubleWritable(result));
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job=new Job(new Configuration());
		job.setJarByClass(ForecastPrice3.class);
		job.setMapperClass(ForecastMapper.class);
		job.setReducerClass(ForecastReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new ForecastPrice3(), args);
		System.exit(exitCode);

	}

}
