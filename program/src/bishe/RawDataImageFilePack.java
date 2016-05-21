package bishe;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.opencv.core.Mat;


public class RawDataImageFilePack extends ImageFilePack {
	
	@Override
	public Writable packOne(Path filePath) throws IOException {
		// TODO Auto-generated method stub
		
		FSDataInputStream in = inputFS.open(filePath);
		Image.RawImage image = new Image.RawImage(in);
		in.close();
		
		return image;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public Class getValueClass() {
		// TODO Auto-generated method stub
		return Image.RawImage.class;
	}
		
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub				
		
	}

	

}
