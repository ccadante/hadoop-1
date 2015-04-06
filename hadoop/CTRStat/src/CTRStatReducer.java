import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CTRStatReducer extends Reducer<Text, IntArrayWritable, Text, Text> {
	
	public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println(key);
		long click = 0;
		long show = 0;
		for (IntArrayWritable value : values) {
			IntWritable[] click_show = new IntWritable[Array.getLength(value.toArray())];
			click_show[0] = (IntWritable) Array.get(value.toArray(), 0);
			click_show[1] = (IntWritable) Array.get(value.toArray(), 1);
			click += Integer.parseInt(click_show[0].toString());
			show += Integer.parseInt(click_show[1].toString());
		}
		
		float ctr = 0.0f;
		if (show != 0)
		{
			ctr = (float)click / show;
		}
		
		context.write(key, new Text("" + click + "," + show + "," + ctr));
	}

}
