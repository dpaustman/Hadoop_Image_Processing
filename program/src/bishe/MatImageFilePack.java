package bishe;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;

public class MatImageFilePack extends ImageFilePack {
	
	static {
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}

	//假定输入的都是本地路径
	@Override
	public Writable packOne(Path filePath) throws IOException {
		// TODO Auto-generated method stub		
		String path = filePath.toString();
		path = path.substring(5, path.length());//去掉路径开头的"file:"字符串		
		
		Mat img = Highgui.imread(path, 1);//输出全部转换为3通道		
		Image.MatImage value = new Image.MatImage(img);
				
		return value;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getValueClass() {
		// TODO Auto-generated method stub
		return Image.MatImage.class;
	}
	
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
