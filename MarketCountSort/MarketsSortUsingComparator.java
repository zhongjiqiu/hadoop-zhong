package org.zkpk.day0822;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarketsSortUsingComparator extends Configured implements Tool {
	static class SortMapper extends Mapper<Object,Text,IntWritable,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String []arr=value.toString().split("\t");
			if(arr.length==2){//判断是否为有效数据
				context.write( new IntWritable(Integer.parseInt(arr[1])),new Text(arr[0]));
			}
		}
		
	}
	static class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable>{

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			String province=null;
			for(Text value:values){
				province=value.toString();
				context.write(new Text(province), key);
			}
			//context方法写在for循环里面，否则会缩小输出结果
		}
		
	}
	static class DecreasingComparator extends IntWritable.Comparator{

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			return -super.compare(a, b);
		}
		
	}
		
	@Override
	public int run(String[] args) throws Exception {
		Job job2=new Job(new Configuration());
		job2.setJarByClass(MarketsSortUsingComparator.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setSortComparatorClass(DecreasingComparator.class);//使用自己定义的排序类
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new MarketsSortUsingComparator(), args);
		System.exit(exitCode);

	}

}
