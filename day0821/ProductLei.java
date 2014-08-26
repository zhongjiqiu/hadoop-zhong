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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.zkpk.day0821.productNum2.productMapper2;
import org.zkpk.day0821.productNum2.productReducer2;
//统计排名前3 的省份共同拥有的农产品类型  ??  江苏、 北京、 山东
public class ProductLei extends Configured implements Tool {
	static class productMapper extends Mapper<Object,Text,Text,IntWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr=value.toString().split(",",-1);
			context.write(new Text(arr[0]+"*"+arr[7]), new IntWritable(1));
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
		
		static class productMapper2 extends Mapper<Text,Text,Text,IntWritable>{

			@Override
			protected void map(Text key, Text value, Context context)
					throws IOException, InterruptedException {
				String province[]=key.toString().split("\\*",-1);
				context.write(new Text(province[1]), new IntWritable(1));
			}
			
		}
		static class productReducer2 extends Reducer<Text,IntWritable,Text,IntWritable>{

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
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job1=new Job(new Configuration());
		job1.setJarByClass(ProductLei.class);
		job1.setMapperClass(productMapper.class);
		job1.setReducerClass(productReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://192.168.1.100:9000/out/21"));
		job1.waitForCompletion(true);
		job1.waitForCompletion(true);
		Job job2=new Job(new Configuration());
		job2.setJarByClass(productNum2.class);
		job2.setMapperClass(productMapper2.class);
		job2.setReducerClass(productReducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.1.100:9000/out/21"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
		
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new ProductLei(), args);
		System.exit(exitCode);
	}

}
