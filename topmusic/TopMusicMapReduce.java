package org.apache.hadoop.mapreduce.app.topmusic;

import java.io.IOException;
import java.net.URI;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopMusicMapReduce {

	public static final int K = 5;
	
	public static class musicMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			if (null == lineValue) {
				return;
			}
			String[] strArr = lineValue.split("\t");
			context.write(new Text(strArr[0] + "\t" + strArr[1]),
					new LongWritable(Long.valueOf(strArr[3])));
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	public static class musicReducer extends
			Reducer<Text, LongWritable, TopMusicWritable, NullWritable> {
		TreeSet<TopMusicWritable> treeSet = new TreeSet<TopMusicWritable>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			if (null == key) {
				return;
			}
			String[] strArr = key.toString().split("\t");
			String songType = strArr[0];
			String songName = strArr[1];
			Long playTimes = 0L;
			for (LongWritable value : values) {
				playTimes += value.get();
			}
			treeSet.add(new TopMusicWritable(songType, songName, playTimes));
			if (treeSet.size() > K) {
				treeSet.remove(treeSet.last());
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (TopMusicWritable top : treeSet) {
				context.write(top, NullWritable.get());
			}
		}
	}

	// Driver
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.7.167:9001");
		String[] args_override = new String[] {"hdfs://192.168.7.167:9000/input",
				"hdfs://192.168.7.167:9000/newout" };
		
        FileSystem fileSystem = FileSystem.get(new URI(args_override[1]), conf);  
        if (fileSystem.exists(new Path(args_override[1]))) {  
            fileSystem.delete(new Path(args_override[1]), true);  
        }  
		
		String[] otherArgs = new GenericOptionsParser(conf, args_override)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopMusic <in> <out>");
			System.exit(2);
		} else {
			args = otherArgs;
		}
		Job job = new Job(conf, "TopMusicMapReduce");
		job.setJarByClass(TopMusicMapReduce.class);
		// 1.input
		Path inputDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputDir);
		// 2.map
		job.setMapperClass(musicMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// 3.reduce
		job.setReducerClass(musicReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(TopMusicWritable.class);
		job.setOutputValueClass(NullWritable.class);
		// 4.output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		int status = new TopMusicMapReduce().run(args);
		System.exit(status);
	}

}
