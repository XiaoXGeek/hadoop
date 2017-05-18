package com.xiaox.multiMR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author XiaoX 找到最高气温，MR1求和，MR2计数，MR3计算平均值
 */
public class MultiMR extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// Configuration Aconf = new Configuration();
		// Configuration Bconf = new Configuration();
		// // 第一个job的配置
		// Job Ajob = Job.getInstance(Aconf);
		// Ajob.setMapperClass(Mapper1.class);
		// Ajob.setReducerClass(Reduce1.class);
		//
		// Ajob.setMapOutputKeyClass(LongWritable.class);// map阶段的输出的key
		// Ajob.setMapOutputValueClass(Text.class);// map阶段的输出的value
		//
		// Ajob.setOutputKeyClass(LongWritable.class);// reduce阶段的输出的key
		// Ajob.setOutputValueClass(Text.class);// reduce阶段的输出的value
		// // 加入控制容器
		// ControlledJob ctrljobA = new ControlledJob(Aconf);
		// ctrljobA.setJob(Ajob);
		// // 第二个作业的配置
		// Job Bjob = Job.getInstance(Bconf);
		// Bjob.setMapperClass(Mapper1.class);
		// Bjob.setReducerClass(Reduce1.class);
		//
		// Bjob.setMapOutputKeyClass(LongWritable.class);// map阶段的输出的key
		// Bjob.setMapOutputValueClass(Text.class);// map阶段的输出的value
		//
		// Bjob.setOutputKeyClass(LongWritable.class);// reduce阶段的输出的key
		// Bjob.setOutputValueClass(Text.class);// reduce阶段的输出的value
		// ControlledJob ctrljobB = new ControlledJob(Bconf);
		// ctrljobB.setJob(Bjob);
		//
		// // 主的控制容器，控制上面的总的两个子作业
		// JobControl jobCtrl = new JobControl("myctrl");
		// // 添加到总的JobControl里，进行控制
		// jobCtrl.addJob(ctrljobA);
		// jobCtrl.addJob(ctrljobB);
		// return 0;

		Configuration conf = new Configuration();
		String inputPath = "/MultiMR/input/duplicate.txt";
		String maxOutputPath = "/MultiMR/output/max/";
		String countOutputPath = "/MultiMR/output/count/";
		String avgOutputPath = "/MultiMR/output/avg/";

		// 删除输出目录(可选,省得多次运行时,总是报OUTPUT目录已存在)
		// HDFSUtil.deleteFile(conf, maxOutputPath);
		// HDFSUtil.deleteFile(conf, countOutputPath);
		// HDFSUtil.deleteFile(conf, avgOutputPath);

		Job job1 = Job.getInstance(conf, "Sum");
		job1.setJarByClass(MultiMR.class);
		job1.setMapperClass(SumMapper.class);
		job1.setCombinerClass(SumReduce.class);
		job1.setReducerClass(SumReduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(maxOutputPath));

		Job job2 = Job.getInstance(conf, "Count");
		job2.setJarByClass(MultiMR.class);
		job2.setMapperClass(CountMapper.class);
		job2.setCombinerClass(CountReduce.class);
		job2.setReducerClass(CountReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(countOutputPath));

		Job job3 = Job.getInstance(conf, "Average");
		job3.setJarByClass(MultiMR.class);
		job3.setMapperClass(AvgMapper.class);
		job3.setReducerClass(AvgReduce.class);
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);

		// 将job1及job2的输出为做job3的输入
		FileInputFormat.addInputPath(job3, new Path(maxOutputPath));
		FileInputFormat.addInputPath(job3, new Path(countOutputPath));
		FileOutputFormat.setOutputPath(job3, new Path(avgOutputPath));

		// 提交job1及job2,并等待完成
		int result  = 0;
		if (job1.waitForCompletion(true) && job2.waitForCompletion(true)) {
			result = job3.waitForCompletion(true) ? 0 : 1;
		}
		return result;
	}

	// 求和
	static class SumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		int sum = 0;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			sum += Integer.parseInt(value.toString());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("TEXT_SUM"), new IntWritable(sum));
		}
	}

	static class SumReduce extends Reducer<LongWritable, IntWritable, Text, IntWritable> {
		public int sum = 0;

		@Override
		protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (IntWritable v : values) {
				sum += v.get();
			}
			context.write(new Text("TEXT_SUM"), new IntWritable(sum));
		}
	}

	// 计数
	static class CountMapper extends Mapper<LongWritable, IntWritable, Text, IntWritable> {

		public int count = 0;

		@Override
		protected void map(LongWritable key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			count++;
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("TEXT_COUNT"), new IntWritable(count));
		}
	}

	static class CountReduce extends Reducer<LongWritable, IntWritable, Text, IntWritable> {

		public int count = 0;

		@Override
		protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (IntWritable v : values) {
				count += v.get();
			}
			context.write(new Text("COUNT"), new IntWritable(count));
		}
	}

	// 平均数
	static class AvgMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		public int sum = 0;
		public int count = 0;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] v = value.toString().split("\t");
			if (v[0].equals("COUNT")) {
				count = Integer.parseInt(v[1]);
			} else {
				sum = Integer.parseInt(v[1]);
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new IntWritable(sum), new IntWritable(count));
		}
	}

	static class AvgReduce extends Reducer<IntWritable, IntWritable, Text, DoubleWritable> {
		public int sum = 0;
		public int count = 0;

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			sum += key.get();
			for (IntWritable v : values) {
				count += v.get();
			}
		}

		@Override
		protected void cleanup(Reducer<IntWritable, IntWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("AVG"), new DoubleWritable(new Double(sum) / count));
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MultiMR(), args);
		System.exit(exitCode);
	}

}
