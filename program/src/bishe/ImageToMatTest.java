package bishe;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.opencv.core.CvType;
import org.opencv.core.Mat;

import bishe.Image.RawImage;

public class ImageToMatTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		Path dirPath = new Path("image/input");
		BufferedWriter out = new BufferedWriter(new FileWriter(
				"tomat_result.txt"));

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(dirPath);
		out.write("path\tsize(byte)\ttomatTime(ms)");
		out.newLine();
		for (FileStatus s : status) {
			// System.out.println(s.getPath());
			FSDataInputStream in = fs.open(s.getPath());
			RawImage image = new RawImage(in);
			in.close();

			long start = System.currentTimeMillis();
			Mat mat = image.toMat();
			long stop = System.currentTimeMillis();

			String msg = String.format("%s\t%d\t%d", s.getPath().toString(),
					s.getLen(), stop - start);
			out.write(msg);
			out.newLine();
		}
		
		Mat m = Mat.eye(3, 3, CvType.CV_8UC1);
		System.out.println(m.dump());
		
		out.close();
	}

}
