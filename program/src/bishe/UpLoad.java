package bishe;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class UpLoad {

	//上传本地文件至hdfs上，返回上传文件的时间。单位为毫秒
	public long run(String localPath, String hdfsPath) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);				
		
		long beginTime = System.currentTimeMillis();
		fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
		long finishTime = System.currentTimeMillis();
		
		return finishTime - beginTime;
	}
	
	//讲本地文件夹下的文件上传到HDFS目录下，传输时间写入到resultPath文件。
	//跳过本地文件下下的子文件夹
	public void uploadAnalyse(String localDirPath, String remoteDirPath, String resultPath) throws IOException {
		File file = new File(localDirPath);
		File[] subfiles = file.listFiles();
	
		BufferedWriter out = null;
				
		try {			
			out = new BufferedWriter(new FileWriter(resultPath));
			String lineSep = System.getProperty("line.separator");
			out.write("filename\tfilesize\tuploadtime" + lineSep);
			for(File f : subfiles) {
				
				if(f.isDirectory()) {
					continue;
				}
				Path remoteFilePath = new Path(remoteDirPath, f.getName()); 
				long time = run(f.getPath(), remoteFilePath.toString());
				
				out.write(f.getName() + '\t' + f.length() + '\t' + time + lineSep);
			}
		} catch(IOException e) {
			e.printStackTrace();
		} finally{
			if(out != null) {
				out.close();
			}
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String localDirPath = "/home/wangbo";
		String remoteDirPath = "uploadTest";
		String resultPath = "result.txt";
		
		UpLoad upLoad = new UpLoad();
		try {
			upLoad.uploadAnalyse(localDirPath, remoteDirPath, resultPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			System.out.println("upload error");
			System.exit(-1);
		}
		
		System.out.println("upload sucess");		
		
	}

}
