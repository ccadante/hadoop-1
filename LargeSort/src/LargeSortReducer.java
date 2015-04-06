import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LargeSortReducer extends Reducer<LongWritable, NullWritable, Text, NullWritable> {
	
	public void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		for (NullWritable  value : values)
		{
			context.write(new Text(key.toString()), NullWritable.get());
		}
	}

}
