package bishe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bishe.Image.RawImage;

public class SequenceFileTest {
	
	//以SequenceFile作为输入，统计每个Mapper处理的图像个数
	public static class MyMapper extends Mapper<Text, RawImage, IntWritable, Text> {
				
		private List<Text> fileNames;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			fileNames = new ArrayList<>();
		}
		
		@Override
		protected void map(Text key, RawImage value,	Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			fileNames.add(new Text(key));
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder builder = new StringBuilder();
			for(Text name : fileNames) {
				builder.append(name.toString() + ";");
			}
			
			Text filePaths = new Text(builder.toString());
			IntWritable cnt = new IntWritable(fileNames.size());
			
			context.write(cnt, filePaths);
			
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();		
		
		String inputPath = "hdfs://localhost:9000/user/wangbo/image/rawdata";
		String outputPath = "hdfs://localhost:9000/user/wangbo/output/seqfile_input_image_per_map";
		
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		Job job = Job.getInstance();
		job.setJar("seqFileTest.jar");
//		job.setJarByClass(SequenceFileTest.class);
		job.setJobName("seqFile input image per map");		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		//添加外部库
		job.addFileToClassPath(new Path("lib/opencv/libopencv_java2410.so"));
		job.addFileToClassPath(new Path("lib/opencv/opencv-2410.jar"));
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		long size = 1024 * 1024 *10;//分片大小设置为10M
		FileInputFormat.setMaxInputSplitSize(job, size);
		
		job.setMapperClass(MyMapper.class);
//		job.setNumReduceTasks(0);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		boolean status = job.waitForCompletion(true);
		System.exit(status ? 1 : 0);
		
	}

}
