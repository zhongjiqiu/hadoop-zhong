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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PricefivedayAvg extends Configured implements Tool {
	static class Mapper1 extends Mapper<Object,Text,Text,DoubleWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[]arr=value.toString().split("\t",-1);
			if(arr[4].equals("É½Î÷")){
				context.write(new Text(arr[0]), new DoubleWritable(Double.parseDouble(arr[1])));
			}
		}
		
	}
	static class Reducer1 extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
				throws IOException, InterruptedException {
			double sum=0;
			double max=Double.MIN_VALUE;
			double min=Double.MAX_VALUE;
			ArrayList<Double> al=new ArrayList<Double>();
			for(DoubleWritable value:values){
				al.add(value.get());
				sum+=value.get();
				max=Math.max(max, value.get());
				min=Math.min(min, value.get());
			}
			if(al.size()==1){
				context.write(key, new DoubleWritable(sum));
			}else if(al.size()==2){
				context.write(key, new DoubleWritable(sum/2));
			}else if(al.size()>2){
				double avg=(sum-max-min)/(al.size()-2);
				avg=Double.parseDouble(String.format("%.2f", avg));
				context.write(key, new DoubleWritable(avg));
			}
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job1=new Job(new Configuration());
		job1.setJarByClass(PricefivedayAvg.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		return job1.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new PricefivedayAvg(), args);
		System.exit(exitCode);

	}

}
