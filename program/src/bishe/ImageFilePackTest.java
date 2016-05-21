package bishe;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.opencv.core.Mat;

public class ImageFilePackTest {
	
	//pack函数功能测试
		//给定输入文件夹和输出文件夹，输出打包后的文件
		public void createRawDataPackTest(String[] args, ImageFilePack packer) throws IOException {
			if(args.length < 2) {
				System.err.printf("Usage %s <inputPath>[inputPath1,inputPath2...] <outputPath>\n", RawDataImageFilePack.class.getSimpleName());
				GenericOptionsParser.printGenericCommandUsage(System.err);
				return;
			}	
			
			String[] inputDirPath = new String[args.length - 1];
			for(int i = 0; i < args.length - 1; i++) {
				inputDirPath[i] = args[i];
			}
			String outputDirPath = args[args.length - 1];		
			
			packer.pack(inputDirPath, outputDirPath);
		}
		
		//以rawPack的输出为该函数的输入路径，打印文件路径和图像大小
		public void packResultTest(String packFilePath, Writable packFileValueType) throws IOException {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(packFilePath), conf);
			
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(packFilePath), conf);
			Text filePath = new Text();
			Writable image = packFileValueType;
			
			
			System.out.println("=========================================================================");
			System.out.printf("%30s%38s\n","filename","size");
			System.out.println("=========================================================================");
			Mat img = null;
			while(reader.next(filePath, image)) {
				if(packFileValueType instanceof Image.RawImage) {
					img = ((Image.RawImage)image).toMat();
				} else if(packFileValueType instanceof Image.MatImage) {
					img = (Mat)packFileValueType;
				}
										
				System.out.printf("%-60s[%4d * %-4d]\n",filePath,img.rows(),img.cols());
			}
			
			reader.close();
		}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//设置存储格式
		//需要更改程序的传入参数
		
		//RawDataImageFilePack参数设置
		ImageFilePack packer = new RawDataImageFilePack();
		Writable value = new Image.RawImage();
		String packFilePath = "image/rawdata";
		
		//MatImageFilePack参数设置
//		String packFilePath = "image/matdata";
//		Writable value = new Image.MatImage();		
//		ImageFilePack packer = new MatImageFilePack();
			

		ImageFilePackTest test = new ImageFilePackTest();
		try {
			long start = System.currentTimeMillis();
			//创建包测试
			test.createRawDataPackTest(args, packer);
			long stop = System.currentTimeMillis();
			
			System.out.printf("time : %d  ms\n", stop - start);
			//解析包测试
//			test.packResultTest(packFilePath, value);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
