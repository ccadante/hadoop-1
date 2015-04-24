import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class BackSearchRatePartitioner extends HashPartitioner<Text, NullWritable> {

	@Override
	public int getPartition(Text key, NullWritable value, int arg2) {
		String[] inputs = key.toString().split("\t");
		return super.getPartition(new Text(inputs[0]), value, arg2);
	}

}
