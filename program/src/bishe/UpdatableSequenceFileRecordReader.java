package bishe;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;



public class UpdatableSequenceFileRecordReader<K, V> extends SequenceFileRecordReader<K, V> {

	private Path updateFilePath;
	private TreeSet<K> deletedKey;
	private int maxUpdateFileSize = 1024 * 1024 * 10;//update文件默认上限为10M
	//删除列表文件名字为输入sequencefile文件路径后加.update。
	//删除列表文件类型为SequenceFile，key必须与输入的文件的key相同。如果包含删除列表文件，其中的key必须实现Comparable接口
	//如在同一个目录下包括  sequenceFile文件seq1，如果有删除列表文件，则文件名为seq1.update。
	//也可以没有删除列表文件。
	public static final String updateFileSuffix = ".update";
		
	//设置update文件大小的上限
	public void setMaxUpdateFileSize(int size) {
		if(size > 0) {
			maxUpdateFileSize = size;
		}
	}
	
	public int getMaxUpdateFileSize() {
		return maxUpdateFileSize;
	}
	
	public static String getUpdateFileSuffix() {
		return updateFileSuffix;
	}
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		super.initialize(split, context);
		
		String dataPath = ((FileSplit)split).getPath().toString();
		updateFilePath = new Path(dataPath + updateFileSuffix);		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		deletedKey = new TreeSet<>();
		
		if(fs.exists(updateFilePath) == false) {
			return;
		} else {			
			long fileSize = fs.getFileStatus(updateFilePath).getLen();
			if(fileSize > maxUpdateFileSize) {
				String err = updateFilePath.toString() + " size is " + fileSize + " but max limit is " + maxUpdateFileSize;
				throw new IOException(err);
			}
						
			SequenceFile.Reader dataReader = null;			
			SequenceFile.Reader updateReader = null;			
			
			try {
				dataReader = new SequenceFile.Reader(fs, new Path(dataPath), conf);			
				updateReader = new SequenceFile.Reader(fs, updateFilePath, conf);
				
				//类型检查
				if(dataReader.getKeyClass() != updateReader.getKeyClass()) {
					String err = "data file key class is " + dataReader.getKeyClassName() + " but update file key class is " + updateReader.getKeyClassName();
					throw new IOException(err);
				}
				
				//因为使用了TreeSet，所以key必须实现Comparable接口
//				if(dataReader instanceof Comparable == false) {
//					throw new IOException("sequence file " + dataPath + " key " + dataReader.getKeyClassName() + " must implements Comparable interface");
//				}
				
				//读入update文件的内容
				K key = (K) updateReader.getKeyClass().newInstance();
				while(updateReader.next(key) != null) {				
					deletedKey.add(key);
					key = (K) updateReader.getKeyClass().newInstance();
				}				
				
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				throw new IOException(e.getMessage());
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				throw new IOException(e.getMessage());
			}  finally {
				if(dataReader != null) {
					dataReader.close();
				}
				if(updateReader != null) {					
					updateReader.close();
				}
			}		
		}			
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(deletedKey.size() == 0) {//无删除列表文件
			return super.nextKeyValue();			
		} else {
			boolean state;
			do {
				state = super.nextKeyValue();
			} while (state && deletedKey.contains(getCurrentKey()));//跳过删除的key
			return state;
		}
	}	
}
