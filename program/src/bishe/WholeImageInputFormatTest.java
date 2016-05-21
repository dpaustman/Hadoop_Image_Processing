package bishe;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencv.core.Core;
import org.opencv.core.Mat;

import bishe.Image.RawImage;

public class WholeImageInputFormatTest {
	
//	static {
//		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
//	}
	
	public static class MyMapper extends Mapper<Text, RawImage, Text, Text> {
		@Override
		protected void map(Text key, RawImage value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Mat img = value.toMat();
			Text info = new Text(img.toString());
			context.write(key, info);
		}
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJobName("WholeImageInputFormatTest");
		job.setJar("wholeimageinputformat.jar");
		
		job.addFileToClassPath(new Path("/lib/libopencv_java2410.so"));
		job.addFileToClassPath(new Path("/lib/opencv-2410.jar"));
		
		job.setInputFormatClass(WholeImageInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("image/input"));
		FileOutputFormat.setOutputPath(job, new Path("output/wholeimageinputformat"));
		
		job.waitForCompletion(true);
		
	}

}
