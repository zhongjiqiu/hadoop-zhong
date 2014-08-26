package org.zkpk.day0821;
//统计排名前3 的省份共同拥有的农产品类型  ??  江苏、 北京、 山东
//对 “黄瓜*北京	1”这样键值对的输入文件，截取 黄瓜 北京做map输出键值对，在reduce里用string对value聚合
//判断字符串里是否indexof排名前3 的省份，如果有则输出，表示该类农产品3个省份都有
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductLei2 extends Configured implements Tool {
	static class proMapper extends Mapper<Text,Text,Text,Text>{

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] aa=key.toString().split("\\*",-1);
			context.write(new Text(aa[0]), new Text(aa[1]));
		}
		
	}
	static class proReducer extends Reducer<Text,Text,Text,NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			String province="";
			for(Text value:values){
				province=province+value.toString();
			}
			if(province.indexOf("江苏")>=0 && province.indexOf("北京")>=0 && province.indexOf("山东")>=0){
				context.write(key, NullWritable.get());
			}
			
		}
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job2=new Job(new Configuration());
		job2.setJarByClass(ProductLei2.class);
		job2.setMapperClass(proMapper.class);
		job2.setReducerClass(proReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path("hdfs://192.168.1.100:9000/out/21"));
		FileOutputFormat.setOutputPath(job2, new Path(args[0]));
		return job2.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new ProductLei2(), args);
		System.exit(exitCode);
	}

}
