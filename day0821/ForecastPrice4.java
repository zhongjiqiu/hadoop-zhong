package org.zkpk.day0821;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ForecastPrice4 extends Configured implements Tool {
	static class ForecastMapper1 extends Mapper<Object,Text,NullWritable,DoubleWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[]arr=value.toString().split("\t",-1);
			double day4_price=(Double.parseDouble(arr[1])-3.53)*(Double.parseDouble(arr[1])-3.53);
			context.write(NullWritable.get(), new DoubleWritable(day4_price));
		}
		
	}

	static class ForecastMapper2 extends Mapper<Object,Text,NullWritable,DoubleWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[]arr=value.toString().split("\t",-1);
			double day5_price=(Double.parseDouble(arr[1])-3.41)*(Double.parseDouble(arr[1])-3.41);
			context.write(NullWritable.get(), new DoubleWritable(day5_price));
		}
		
	}
	static class ForecastReducer extends Reducer<NullWritable,DoubleWritable,Text,DoubleWritable>{

		@Override
		protected void reduce(NullWritable key, Iterable<DoubleWritable> values,Context context)
				throws IOException, InterruptedException {
			double result=0;
			for(DoubleWritable value:values){
				result+=value.get();
			}
			context.write(new Text("Æ½·½Îó²îºÍ"), new DoubleWritable(result));
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job=new Job(new Configuration());
		job.setJarByClass(ForecastPrice4.class);
		MultipleInputs.addInputPath(job, new Path("hdfs://192.168.1.100:9000/output/082606"), TextInputFormat.class,ForecastMapper1.class);
		MultipleInputs.addInputPath(job, new Path("hdfs://192.168.1.100:9000/output/082607"), TextInputFormat.class,ForecastMapper2.class);
		job.setReducerClass(ForecastReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new ForecastPrice4(), args);
		System.exit(exitCode);

	}

}
