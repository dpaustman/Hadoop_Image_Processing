package bishe;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;



public class UpdatableSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V>{
	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		// TODO Auto-generated method stub
		UpdatableSequenceFileRecordReader<K, V> reader = new UpdatableSequenceFileRecordReader<>();
		try {
			reader.initialize(split, context);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			throw new IOException(e.getMessage());
		}
		return reader;
	}
}
