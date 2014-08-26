package org.zkpk.day0821;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ForecastPrice extends Configured implements Tool {
	static class ForecastMapper extends Mapper<Object,Text,Text,DoubleWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[]arr=value.toString().split("\t",-1);
			if(arr[0].equals("»Æ¹Ï") && arr[4].equals("É½Î÷")){
				context.write(new Text(arr[0]), new DoubleWritable(Double.parseDouble(arr[1])));
			}
		}
		
	}
	static class ForecastReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
				throws IOException, InterruptedException {
			ArrayList<Double> prices=new ArrayList<Double>();
			double sum=0;
			for(DoubleWritable value:values){
				prices.add(value.get());
				sum+=value.get();
			}
			if(prices.size()==1){
				context.write(key, new DoubleWritable(sum));
			}
			if(prices.size()>1){
				double avg=sum/prices.size();
				avg=Double.parseDouble(String.format("%.2f", avg));
				context.write(key, new DoubleWritable(avg));
			}
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job=new Job(new Configuration());
		job.setJarByClass(ForecastPrice.class);
		job.setMapperClass(ForecastMapper.class);
		job.setReducerClass(ForecastReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new ForecastPrice(), args);
		System.exit(exitCode);

	}

}
