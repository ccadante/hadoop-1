import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class TurnningRatePartitioner extends HashPartitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int arg2) {
		String[] inputs = key.toString().split("_");
		return super.getPartition(new Text(inputs[0]), value, arg2);
	}

}
