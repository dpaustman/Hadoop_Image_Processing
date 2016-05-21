package bishe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;

public class Image {

	// 图像的内容原样存储，通过函数解析生成Mat。
	// 文件格式：文件大小、文件原始数据。假定单幅图像的大小不超过4G。
	public static class RawImage implements Writable {

		static {
			System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		}

		private byte[] rawdata;

		public RawImage() {
			// TODO Auto-generated constructor stub
			rawdata = null;
		}

		public RawImage(InputStream in) throws IOException {
			int off = 0;
			int readBytes = 0;
			int totalBytes = in.available();
			rawdata = new byte[totalBytes];
			//调用FSDataInputStream的read方法，一次只能读65536字节。
			//调用FileInputStream的read方法则可以正确读所有字节
			//因此需要循环处理			
			while( (readBytes = in.read(rawdata, off, totalBytes - off) ) > 0) {
				off += readBytes;
			}
			
			if(off != totalBytes) {
				String err = String.format("read %d bytes but total bytes is %d", off, totalBytes);
				throw new IOException(err);
			}
		}

		public RawImage(String filePath) throws IOException {

			FileInputStream in = new FileInputStream(new File(filePath));
			int fileSize = in.available();
			rawdata = new byte[fileSize];
			in.read(rawdata);
			in.close();
		}

		public void set(byte[] data) {
			rawdata = data;
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			// TODO Auto-generated method stub
			int fileSize = input.readInt();
			rawdata = new byte[fileSize];
			input.readFully(rawdata);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			// TODO Auto-generated method stub
			output.writeInt(rawdata.length);
			output.write(rawdata);
		}

		// 图像文件的大小
		public int getSize() {

			if (rawdata == null) {
				return 0;
			} else {
				return rawdata.length;
			}
		}

		public byte[] getRawData() {
			return rawdata;
		}
		
		// 将原始图像文件解析成易于处理的Mat类型。
		// flags同imread中的flags。flags>0：三通道；flags=0:单通道；flags<0：具有alpha通道的原始图像。
		public Mat toMat(int flags) {
			Mat img = new Mat();
			if (rawdata == null) {
				return img;
			} else {
				MatOfByte buff = new MatOfByte(rawdata);
				img = Highgui.imdecode(buff, flags);				
				return img;
			}
		}
		
		// 将原始图像文件解析成三通道的Mat类型。
		public Mat toMat() {
			return toMat(Highgui.CV_LOAD_IMAGE_COLOR);
		}
		
		//将图像mat编码为后缀为ext的图像文件
		//ext为图像后缀如“.jpg",".bmp"等
		public static RawImage toImage(Mat mat, String ext) {
			RawImage img = new RawImage();
			if(mat == null) {
				return img;
			} else {
				MatOfByte buff = new MatOfByte();
				Highgui.imencode(ext, mat, buff);
				img.set(buff.toArray());
				return img;
			}			
		}
	
		//默认将mat编码为jpg格式
		public static RawImage toImage(Mat mat) {
			return toImage(mat, ".jpg");
		}
	}

	// 只存储图像解析后的像素信息
	// 文件格式：图像的行、图像的列、存储的数据类型(CV_8UC1、CV_32FC1等）、像素数据
	public static class MatImage extends Mat implements Writable {

		static {
			System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		}

		public MatImage() {
			// TODO Auto-generated constructor stub
			super();
		}

		public MatImage(Mat mat) {
			super(mat.nativeObj);
		}

		// 创建矩阵，分配存储空间，但是矩阵中的值是随机的，需要后面初始化
		public MatImage(int rows, int cols, int type) {
			super(rows, cols, type);
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			// TODO Auto-generated method stub
			int rows = input.readInt();
			int cols = input.readInt();
			int type = input.readInt();
			int channels = CvType.channels(type);// 通道数
			int byteCnt = CvType.ELEM_SIZE(type) / channels;// 存储的数据类型(byte:1;short:2;float:4;double:8)

			create(rows, cols, type);
			// 填充数据
			byte[] byteBuff = new byte[channels];
			short[] shortBuff = new short[channels];
			float[] floatBuff = new float[channels];
			double[] doubleBuff = new double[channels];

			for (int row = 0; row < rows; row++) {
				for (int col = 0; col < cols; col++) {
					if (byteCnt == 1) {
						for (int i = 0; i < channels; i++) {
							byteBuff[i] = input.readByte();
						}
						put(row, col, byteBuff);
					} else if (byteCnt == 2) {

						for (int i = 0; i < channels; i++) {
							shortBuff[i] = input.readShort();
						}
						put(row, col, shortBuff);
					} else if (byteCnt == 4) {

						for (int i = 0; i < channels; i++) {
							floatBuff[i] = input.readFloat();
						}
						put(row, col, floatBuff);
					} else if (byteCnt == 8) {

						for (int i = 0; i < channels; i++) {
							doubleBuff[i] = input.readDouble();
						}
						put(row, col, doubleBuff);
					} else {
						throw new java.lang.UnsupportedOperationException(
								"Unsupported CvType value: " + type);
					}
				}
			}

		}

		@Override
		public void write(DataOutput output) throws IOException {
			// TODO Auto-generated method stub
			output.writeInt(rows());
			output.writeInt(cols());
			output.writeInt(type());

			int type = type();
			int channels = channels();
			int byteCnt = CvType.ELEM_SIZE(type) / channels;// 存储的数据类型(byte:1;short:2;float:4;double:8)
			int rows = rows();
			int cols = cols();

			// 写数据
			byte[] byteBuff = new byte[channels];
			short[] shortBuff = new short[channels];
			float[] floatBuff = new float[channels];
			double[] doubleBuff = new double[channels];

			for (int row = 0; row < rows; row++) {
				for (int col = 0; col < cols; col++) {
					if (byteCnt == 1) {
						get(row, col, byteBuff);
						for (int i = 0; i < channels; i++) {
							output.writeByte(byteBuff[i]);
						}
					} else if (byteCnt == 2) {
						get(row, col, shortBuff);
						for (int i = 0; i < channels; i++) {
							output.writeShort(shortBuff[i]);
						}
					} else if (byteCnt == 4) {
						get(row, col, floatBuff);
						for (int i = 0; i < channels; i++) {
							output.writeFloat(floatBuff[i]);
						}
					} else if (byteCnt == 8) {
						get(row, col, doubleBuff);
						for (int i = 0; i < channels; i++) {
							output.writeDouble(doubleBuff[i]);
						}
					} else {
						throw new java.lang.UnsupportedOperationException(
								"Unsupported CvType value: " + type);
					}
				}
			}
		}

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in1 = fs.open(new Path("image/input/1.jpg"));
		RawImage img = new RawImage(in1);
		in1.close();
		
		Mat mat1 = img.toMat();
		System.out.println(mat1);
		
		Imgproc.cvtColor(mat1, mat1, Imgproc.COLOR_BGR2GRAY);
		RawImage gray = RawImage.toImage(mat1);
		
		FileOutputStream out1 = new FileOutputStream("gray.jpg");
		out1.write(gray.getRawData());
		out1.close();
		
		System.exit(0);
		
		
		// RawImage类测试
		String path = "1.jpg";
		FileInputStream in = new FileInputStream(new File(path));		
		System.out.println(in.available());
		RawImage image = new RawImage(in);
		in.close();
		
		Mat m = image.toMat();
		System.out.println(m);
		System.out.println(image.getSize());
		
		System.out.println(img.getSize());
		System.out.println(image.getSize());
		System.out.println(Arrays.equals(img.getRawData(), image.getRawData()));
		
		byte[] sbyte = image.getRawData();
		byte[] dbyte = img.getRawData();
		
		for(int i = 0; i < sbyte.length; i++) {
			if(sbyte[i] != dbyte[i]) {
				System.out.printf("%d:[%d,%d]\n",i,sbyte[i],dbyte[i]);
				break;
			}
		}
		
		System.exit(0);
		
		Imgproc.Laplacian(m, m, 3);
		Imgproc.threshold(m, m, 10, 255, Imgproc.THRESH_BINARY);
		RawImage edge = null;
		edge = RawImage.toImage(m);
		FileOutputStream out = new FileOutputStream(new File("result.jpg"));
		out.write(edge.getRawData());
		out.close();
				
		System.exit(0);
		
		// MatImage类测试
		System.out.println("==========================");
		
		Mat mat = Highgui.imread(path, 1);
		MatImage src = new Image.MatImage(mat);
		
		System.out.println(src);
		System.out.println(mat);
		Imgproc.cvtColor(src, src, Imgproc.COLOR_BGR2GRAY);
//		mat = new Mat();
		System.out.println(src);
		System.out.println(mat);
		

		MatImage s = new Image.MatImage(2, 2, CvType.CV_32FC3);
		float[] value = { 1, 2.2F, -4.3F };
		s.put(0, 0, value);
		System.out.println(s);
		System.out.println(s.dump());
	}

}
