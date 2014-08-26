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
//实现 统计每种农产品有几个菜市场在销售
public class PriceWave extends Configured implements Tool {
	static class PriceMapper extends Mapper<Object,Text,Text,DoubleWritable>{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr=value.toString().split("\t",-1);
			if(arr[4].equals("山西") ){
				context.write(new Text(arr[0]), new DoubleWritable(Double.parseDouble(arr[1])));
			}
			//equals 比较两个字符串的内容是否相等
		}
		
	}
	static class PriceReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

		ArrayList<Double> al=new ArrayList<Double>();
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
				throws IOException, InterruptedException {
			double sum=0,avg=0,ss=0;
			double max=Double.MIN_VALUE;
			double min=Double.MAX_VALUE;
			for(DoubleWritable value:values){
				al.add(value.get());
				sum+=value.get();
				max=Math.max(max, value.get());
				min=Math.min(min, value.get());
			}
			if(al.size()==1){//用arraylist的size判断该农产品有几个市场来卖，方便求均值
				context.write(key, new DoubleWritable(al.get(0)));
			}
			else if(al.size()==2){
				ss=Double.parseDouble(String.format("%.2f", (max+min)/2));//控制有效数字输出位数或精度
				context.write(key, new DoubleWritable(ss));
			}
			else if(al.size()>2){
				avg=(sum-max-min)/(al.size()-2);//PAVG=(PM1+PM2+...+PMn-max(P)-min(P))/(N-2)				
				avg=Double.parseDouble(String.format("%.2f", avg));//控制有效数字输出位数或精度
				context.write(key, new DoubleWritable(avg));
			}
			
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job1=new Job(new Configuration());
		job1.setJarByClass(PriceWave.class);
		job1.setMapperClass(PriceMapper.class);
		job1.setReducerClass(PriceReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		return job1.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new PriceWave(), args);
		System.exit(exitCode);

	}

}
