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

public abstract class ImageFilePack {
	
	protected FileSystem inputFS;
	protected FileSystem outputFS;
	
	
	// 将给定文件夹下的图片打包为一个序列文件
	// 给定的文件夹下子文件夹不会被遍历，只会存储opencv支持的图像格式的文件。
	public void pack(String[] inputDirPath, String outputDirPath)
			throws IOException {
		
		Configuration conf = new Configuration();
		inputFS  = FileSystem.get(URI.create(inputDirPath[0]), conf);
		outputFS = FileSystem.get(URI.create(outputDirPath), conf);

		//过滤掉opencv不支持的文件格式
		Path[] inputPath = new Path[inputDirPath.length];
		for (int i = 0; i < inputDirPath.length; i++) {
			inputPath[i] = new Path(inputDirPath[i]);
		}

		FileStatus[] status = inputFS.listStatus(inputPath, new PathFilter() {

			@Override
			public boolean accept(Path path) {
				// TODO Auto-generated method stub
//				final String[] acceptFormat = { ".bmp", ".dib", ".jpg",
//						".jpeg", ".jpe", ".png", ".pbm", ".pgm", ".ppm", ".sr",
//						".ras", ".tiff", ".tif" };
//
//				String filePath = path.getName();
//				for (String format : acceptFormat) {
//					if (filePath.endsWith(format)) {
//						return true;
//					}
//				}
////				return false;
				
				return true;
			}
		});
				
		
		SequenceFile.Writer writer = new SequenceFile.Writer(outputFS, conf,
				new Path(outputDirPath), Text.class, getValueClass());

		Path[] filePath = FileUtil.stat2Paths(status);
		for (Path path : filePath) {
			writer.append(new Text(path.toString()), packOne(path));
		}

		writer.close();

	}

	//给定输入的文件路径，返回Hadoop支持的存储类型
	public abstract Writable packOne(Path filePath) throws IOException;
	//设置SequenceFile.Write类的Value类型
	@SuppressWarnings("rawtypes")
	public abstract Class getValueClass();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
