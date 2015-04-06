import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

class SortFileInputFormat extends InputFormat<LongWritable, NullWritable>
{
	private TextInputFormat _rawInputFormat = new TextInputFormat();
	
	
	@Override
	public List<InputSplit> getSplits(JobContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return _rawInputFormat.getSplits(context);
	}

	@Override
	public RecordReader<LongWritable, NullWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		final RecordReader<LongWritable, Text> textRecordReader = 
				_rawInputFormat.createRecordReader(split, context);
		return new RecordReader<LongWritable, NullWritable>(){

			@Override
			public void initialize(InputSplit split,
					TaskAttemptContext context) throws IOException,
					InterruptedException {
				textRecordReader.initialize(split, context);
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return textRecordReader.nextKeyValue();
			}

			@Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return new LongWritable(
						Long.parseLong(textRecordReader.getCurrentValue().toString()));
			}

			@Override
			public NullWritable getCurrentValue() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return NullWritable.get();
			}

			@Override
			public float getProgress() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return textRecordReader.getProgress();
			}

			@Override
			public void close() throws IOException {
				textRecordReader.close();
			}
			
		};
	}
}