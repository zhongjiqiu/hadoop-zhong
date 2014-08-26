package org.zkpk.day0822;
//��Arraylist��Map���ʵ�ֵĽ������
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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

public class MarketSortUsingCollections extends Configured implements Tool {
	static class SortMapper extends Mapper<Object,Text,IntWritable,Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String []arr=value.toString().split("\t");
			if(arr.length==2){//�ж��Ƿ�Ϊ��Ч����
				context.write(new IntWritable(Integer.parseInt(arr[1])), new Text(arr[0]));
			}
		}
		
	}
	static class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable>{
		ArrayList<Integer> list1=new ArrayList<Integer>();
		HashMap<Integer,ArrayList<String>> map=new HashMap<Integer,ArrayList<String>>();
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			list1.add(key.get());//list1����г��ĸ�������MRĬ�϶Լ�������������
			ArrayList<String> list2=new ArrayList<String>();
			
			for (Text value:values){
				list2.add(value.toString());//���ʡ��				
			}
			map.put(new Integer(key.get()), list2);//����г�������ʡ�ݵĶ�Ӧ��ֵ��
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			for(int i=list1.size()-1;i>=0;i--){
				ArrayList<String> list3=(map.get(list1.get(i)));
				for(String province:list3){
					context.write(new Text(province),new IntWritable(list1.get(i).intValue()));
				}//list1.get(i)���ص�������Ϊi��Integer���ͣ�������Ҫint���ͣ���intValue����Integerת��Ϊint
				
			}
		}
		
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job2=new Job(new Configuration());
		job2.setJarByClass(MarketSortUsingCollections.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		return job2.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new MarketSortUsingCollections(), args);
		System.exit(exitCode);
	}

}
