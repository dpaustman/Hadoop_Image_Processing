package bishe;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UpdatableSequenceFileInputFormatTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Path input  = new Path("output/sequenceFile");
		Path output = new Path("output/updatableResult");
		
		Job job = Job.getInstance();
		job.setJar("updatableSeqFileTest.jar");
		job.setJobName("UpdatableSequenceFileInputFormatTest");
		job.setInputFormatClass(UpdatableSequenceFileInputFormat.class);
		UpdatableSequenceFileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		if(job.waitForCompletion(true)) {
			System.out.println("succeed");
		} else {
			System.out.println("failed");
		}
	}

}
