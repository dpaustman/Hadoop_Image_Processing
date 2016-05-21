package bishe;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;

import com.sun.org.apache.bcel.internal.generic.NEW;

public class ImageTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		
		Mat[] m = new Mat[3];
		m[0] = new Mat(2, 2, CvType.CV_8UC3);
		m[1] = new Mat(2,2,CvType.CV_32FC1);
		m[2] = Mat.eye(2, 2, CvType.CV_16SC1);
		
		Image.MatImage[] image = new Image.MatImage[m.length];
		for(int i = 0; i < image.length; i++) {
			image[i] = new Image.MatImage(m[i]);
		}
		
		for(Image.MatImage img : image) {
			System.out.println(img);
		}
		System.out.println("============================================================");
		
		//写数据
		String fileName = "image.dat";
		DataOutputStream writer = new DataOutputStream(new FileOutputStream(fileName));
		for(Image.MatImage img : image) {
			img.write(writer);
		}
		writer.close();
		
		//读数据
		DataInputStream reader = new DataInputStream(new FileInputStream(fileName));
		Image.MatImage data = new Image.MatImage();
		
		while(reader.available() > 0) {
			data.readFields(reader);
			System.out.println(data);
		}
		
	}

}
