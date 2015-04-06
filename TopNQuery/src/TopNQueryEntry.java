import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;


public class TopNQueryEntry {
	
	public static final int DEFAULT_TOP_NUM = 5;
	public static final String CONF_TOP_NUM_KEY_NAME = "topNum";
	
	public static void main(String[] args) throws Exception {
		
		String input = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/top_query/input/";
		String output = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/top_query/output/";
		
		Configuration conf = new Configuration();
		
		conf.setInt(CONF_TOP_NUM_KEY_NAME, 10);
		
		PropertyConfigurator.configure("log4j.properties");
	
	    URI uri = new URI(output);
	    FileSystem fs = FileSystem.get(uri, conf, "work");
	    if (fs.delete(new Path(uri), true))
	    {
	    	System.out.println("Delete output dir first");
	    }

		Job job = Job.getInstance(conf, "TopNQuery");
		job.setJarByClass(TopNQueryEntry.class);
		job.setMapperClass(TopNQueryMapper.class);
		job.setReducerClass(TopNQueryReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		if (!job.waitForCompletion(true))
			return;
	}

}
