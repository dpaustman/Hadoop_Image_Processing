package bishe;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;


public class Seq2MapFile {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {				
		
		if(args.length < 2 || args.length > 3) {
			System.err.println("Usage: Seq2MapFile <inputPath> <outputPath> [reduceTaskNum]");
			System.exit(-1);
		}		
		
		int reduceTaskNum = 10;
		if (args.length == 3) {
			reduceTaskNum = Integer.parseInt(args[2]);
		}
		
		Path inputPath  = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Job job = Job.getInstance();
		job.setJobName("Seq2MapFile");
		job.setNumReduceTasks(reduceTaskNum);		
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		
		SequenceFileInputFormat.addInputPath(job, inputPath);
		MapFileOutputFormat.setOutputPath(job, outputPath);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
	}

}
