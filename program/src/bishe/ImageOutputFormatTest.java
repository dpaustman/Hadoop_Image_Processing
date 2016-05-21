package bishe;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import bishe.Image.RawImage;


public class ImageOutputFormatTest {
	
	public static class RGB2GrayMapper extends Mapper<Text, RawImage, Text, RawImage> {
		
		@Override
		protected void map(Text key, RawImage value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Path path = new Path(key.toString());
			Text fileName = new Text(path.getName());
					
			Mat img = value.toMat();
			Imgproc.cvtColor(img, img, Imgproc.COLOR_BGR2GRAY);
			
			context.write(fileName, RawImage.toImage(img));			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJobName("ImageOutputTest");
		job.setJar("imageoutputformat.jar");
		
		job.addFileToClassPath(new Path("/lib/libopencv_java2411.so"));
		job.addFileToClassPath(new Path("/lib/opencv-2411.jar"));
		
		job.setInputFormatClass(WholeImageInputFormat.class);
		job.setOutputFormatClass(ImageOutputFormat.class);
		job.setMapperClass(RGB2GrayMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(RawImage.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path("image/input"));
		FileOutputFormat.setOutputPath(job, new Path("output/imageoutputformat"));
		
		job.waitForCompletion(true);
	}

}
