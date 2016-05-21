package bishe;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.hamcrest.core.Is;

public class UpdatableSequenceFileTool {

	private TreeSet<Comparable> set;
	private Path updateFilePath;// 删除列表文件
	private Path dataPath;// seqFile数据文件
	private Configuration conf;
	private FileSystem fs;
	private Class keyClass;

	// inputPath为含有data的seqFile文件路径
	// 如果该路径下面没有删除列表文件就会在添加删除key之后自动创建
	// keyClass为写入到删除列表文件的key类型，value为NULLWritable类型
	public UpdatableSequenceFileTool(String inputPath) throws IOException {

		dataPath = new Path(inputPath);
		updateFilePath = new Path(inputPath
				+ UpdatableSequenceFileRecordReader.updateFileSuffix);
		set = new TreeSet<>();
		conf = new Configuration();
		fs = FileSystem.get(conf);

		if (fs.exists(dataPath) == false) {
			throw new IOException(dataPath + " doesn't exists");
		}

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, dataPath, conf);
		keyClass = reader.getKeyClass();
		reader.close();

		load();
	}

	// 导入删除列表文件
	private void load() throws IOException {

		if (fs.exists(updateFilePath) == false) {
			return;
		}

		SequenceFile.Reader updateReader = null;
		try {
			// 载入删除列表文件
			updateReader = new SequenceFile.Reader(fs, updateFilePath, conf);
			if (updateReader.getKeyClass() != keyClass) {
				throw new IOException("updatefile key is "
						+ updateReader.getKeyClassName()
						+ " but datafile key is " + keyClass.getName());
			}

			Comparable key;
			key = (Comparable) keyClass.newInstance();

			while (updateReader.next(key) != null) {
				set.add(key);
				key = (Comparable) updateReader.getKeyClass().newInstance();
			}

		} catch (Exception e) {
			throw new IOException("load update file "
					+ updateFilePath.toString() + " error");
		} finally {
			if (updateReader != null) {
				updateReader.close();
			}
		}
	}

	// 添加删除的key
	public void addDelKey(Comparable key) throws IOException {

		if (key.getClass() != keyClass) {
			throw new IOException("datafile key is " + keyClass.getName()
					+ " but input del key is " + key.getClass().getName());
		}

		set.add(key);
	}

	// 移除删除的key
	public void rmvDelKey(Comparable key) {
		set.remove(key);
	}

	// 移除所有待删除的key
	public void rmvAllDelKey() {
		set.clear();
	}

	// 将更新的数据写入到删除列表文件中
	public void writeToFile() throws IOException {

		if (set.size() == 0) {// 删除列表被清空，删除删除列表文件
			fs.deleteOnExit(updateFilePath);
			return;
		}

		SequenceFile.Writer writer = null;

		try {
			writer = new SequenceFile.Writer(fs, conf, updateFilePath,
					keyClass, NullWritable.class);
			NullWritable value = NullWritable.get();

			for (Comparable key : set) {
				writer.append(key, value);
			}

		} finally {
			// TODO: handle exception
			if (writer != null) {
				writer.close();
			}
		}
	}

	public static void main(String[] args) throws IOException {

		String seqFilePath = "output/sequenceFile";
		UpdatableSequenceFileTool tool = new UpdatableSequenceFileTool(
				seqFilePath);

		int[] delKey = { 1, 4, 6, 7 };

		for (int v : delKey) {
			tool.addDelKey(new LongWritable(v));
		}

//		tool.rmvAllDelKey();
		tool.writeToFile();

	}

}
