package org.zkpk.day0821;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UpdatedOfproductNum2 extends Configured implements Tool {
	static class productMapper2 extends Mapper<Text,Text,Text,IntWritable>{

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String province[]=key.toString().split("\\*",-1);
			context.write(new Text(province[1]), new IntWritable(1));
		}
		
	}
	static class productReducer2 extends Reducer<Text,IntWritable,Text,IntWritable>{
		ArrayList<Integer> list1=new ArrayList();
		HashMap<Integer,ArrayList<String>> map =new HashMap();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			
			context.write(key,new IntWritable(sum));
			
		}
		//value降序输出木有实现啊，对于集合的概念很模糊啊
		//我这里全都没有集合，但是在Linux环境下用命令行实现的降序输出，文件比较小或者实时性不强或许可以这样，否则还得在
		//mapreduce中实现排序
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job2=new Job(new Configuration());
		job2.setJarByClass(UpdatedOfproductNum2.class);
		job2.setMapperClass(productMapper2.class);
		job2.setReducerClass(productReducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.1.100:9000/out/11"));
		FileOutputFormat.setOutputPath(job2, new Path(args[0]));
		return job2.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new UpdatedOfproductNum2(), args);
		System.exit(exitCode);

	}

}
