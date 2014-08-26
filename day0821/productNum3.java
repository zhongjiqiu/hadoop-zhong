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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class productNum3 extends Configured implements Tool {
	static class Join1Mapper extends Mapper<Object,Text,Text,IntWritable>{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr=value.toString().split("\t",-1);
			context.write(new Text(arr[0]), new IntWritable(1));
		}		
	}
	
	static class Join2Mapper extends Mapper<Object,Text,Text,IntWritable>{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr=value.toString();
			context.write(new Text(arr), new IntWritable(1));
		}
		//china-province valueÿһ�м�¼��Ϊvalue
	}
	static class JoinReducer extends Reducer<Text,IntWritable,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			if(key.toString().indexOf("ʡ��")>=0||key.toString().indexOf("����")>=0||key.toString().indexOf("���")>=0||key.toString().indexOf("�Ϻ�")>=0||key.toString().indexOf("����")>=0){
				sum++;
			}//province��û��4��ֱϽ�У�
			if(sum==1){//˵��ֻ������һ��
				context.write(key, new Text("û��ͳ�Ƶ��г�"));
			}
								
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job3=new Job(new Configuration());
		job3.setJarByClass(productNum3.class);
		job3.setReducerClass(JoinReducer.class);
		MultipleInputs.addInputPath(job3, new Path("hdfs://192.168.1.100:9000/output/0821"), TextInputFormat.class,Join1Mapper.class);
		MultipleInputs.addInputPath(job3, new Path("hdfs://192.168.1.100:9000/input/new-china-province.txt"), TextInputFormat.class,Join2Mapper.class);
		FileOutputFormat.setOutputPath(job3, new Path(args[0]));
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		return job3.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new productNum3(), args);
		System.exit(exitCode);
	}

}
