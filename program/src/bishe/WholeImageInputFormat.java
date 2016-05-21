package bishe;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import bishe.Image.RawImage;

public class WholeImageInputFormat extends FileInputFormat<Text, RawImage> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	 public RecordReader<Text, RawImage> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		WholeImageRecordReader reader = new WholeImageRecordReader();
		reader.initialize(split, context);
		return reader;
	}

	public class WholeImageRecordReader extends RecordReader<Text, RawImage> {

		private boolean finished = false;
		private RawImage image;
		private Text filePath;

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}
		
		//输入图像的完整路径
		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return filePath;
		}

		@Override
		public RawImage getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return image;
		}
		
		

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (finished) {
				return 1;
			} else {
				return 0;
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit fileSplit = (FileSplit) split;
			Path path = fileSplit.getPath();
			filePath = new Text(path.toString());

			Configuration conf = context.getConfiguration();
			FSDataInputStream in = FileSystem.get(conf).open(path);
//			int size = in.available();
//			context.getCounter("availablebytes",path.toString()).increment(size);
			image = new RawImage(in);
			in.close();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (finished) {
				return false;
			} else {
				finished = true;
				return true;
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
