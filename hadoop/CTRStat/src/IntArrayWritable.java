import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
	 public IntArrayWritable() {
	 super(IntWritable.class);
	 }

	 public IntArrayWritable(String[] strings) {
		 super(IntWritable.class);
		 IntWritable[] texts = new IntWritable[strings.length];
		 for (int i = 0; i < strings.length; i++) {
			 texts[i] = new IntWritable(Integer.parseInt(strings[i]));
		 }
		 set(texts);
		 }
	}