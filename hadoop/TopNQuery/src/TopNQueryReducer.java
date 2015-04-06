import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopNQueryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		
		context.write(key, new IntWritable(count));
	}
}
