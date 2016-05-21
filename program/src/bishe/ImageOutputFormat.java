package bishe;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bishe.Image.RawImage;

public class ImageOutputFormat extends FileOutputFormat<Text, RawImage> {

	@Override
	public RecordWriter<Text, RawImage> getRecordWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Path outputDir = getDefaultWorkFile(context, "");// 获取缺省的文件路径

		return new ImageRecordWriter(outputDir, context.getConfiguration());
	}

	protected class ImageRecordWriter extends RecordWriter<Text, RawImage> {

		private Path basePath;
		private FileSystem fs;

		public ImageRecordWriter(Path basePath, Configuration conf)
				throws IOException {
			// TODO Auto-generated constructor stub
			this.basePath = basePath;
			fs = basePath.getFileSystem(conf);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		public void write(Text fileName, RawImage img) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Path outputPath = new Path(basePath, fileName.toString());
			FSDataOutputStream out = fs.create(outputPath);
			out.write(img.getRawData());
			out.close();
		}

	}

}
