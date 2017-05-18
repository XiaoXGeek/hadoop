package com.xiaox.multiMR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiMR extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration Aconf = new Configuration();
		Configuration Bconf = new Configuration();
		// 第一个job的配置
		Job Ajob = Job.getInstance(Aconf);
		Ajob.setMapperClass(Mapper1.class);
		Ajob.setReducerClass(Reduce1.class);

		Ajob.setMapOutputKeyClass(LongWritable.class);// map阶段的输出的key
		Ajob.setMapOutputValueClass(Text.class);// map阶段的输出的value

		Ajob.setOutputKeyClass(LongWritable.class);// reduce阶段的输出的key
		Ajob.setOutputValueClass(Text.class);// reduce阶段的输出的value
		// 加入控制容器
		ControlledJob ctrljobA = new ControlledJob(Aconf);
		ctrljobA.setJob(Ajob);
		//jairu
		// 第二个作业的配置
		Job Bjob = Job.getInstance(Bconf);
		Bjob.setMapperClass(Mapper1.class);
		Bjob.setReducerClass(Reduce1.class);

		Bjob.setMapOutputKeyClass(LongWritable.class);// map阶段的输出的key
		Bjob.setMapOutputValueClass(Text.class);// map阶段的输出的value

		Bjob.setOutputKeyClass(LongWritable.class);// reduce阶段的输出的key
		Bjob.setOutputValueClass(Text.class);// reduce阶段的输出的value
		ControlledJob ctrljobB = new ControlledJob(Bconf);
		ctrljobB.setJob(Bjob);

		// 主的控制容器，控制上面的总的两个子作业
		JobControl jobCtrl = new JobControl("myctrl");
		// 添加到总的JobControl里，进行控制
		jobCtrl.addJob(ctrljobA);
		jobCtrl.addJob(ctrljobB);
		return 0;
	}

	static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
		}
	}

	static class Reduce1 extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(arg0, arg1, context);
		}
	}

	static class Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
		}
	}

	static class Reduce2 extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(arg0, arg1, context);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MultiMR(), args);
		System.exit(exitCode);
	}

}
