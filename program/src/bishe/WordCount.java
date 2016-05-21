package bishe;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;


public class WordCount {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private LongWritable one = new LongWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()) {
				context.write(new Text(tokenizer.nextToken()), one);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJobName("world count");
		job.setJar("wordcount.jar");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(LongSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("input/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output/wordcount"));
		
		job.waitForCompletion(true);
	}

}
