import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;


public class TurnningRateEntry extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		System.exit(ToolRunner.run(new TurnningRateEntry(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		String input = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/turnning_rate/input/";
		String output = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/turnning_rate/output/";
		
		Configuration conf = getConf();
		
	    if (true)
	    {
	        //开启下面两句为集群模式
	        conf.set("mapreduce.framework.name", "yarn");
	        conf.set("yarn.resourcemanager.address", "cp01-ma-eval-001.cp01.baidu.com:8032");	
	        conf.set("fs.default.name", "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020");
	        conf.set("yarn.resourcemanager.scheduler.address", "cp01-ma-eval-001.cp01.baidu.com:8030");
	        conf.set("mapreduce.app-submission.cross-platform", "true");
	    }
	    
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(input), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
				
	    URI uri = new URI(output);
	    if (fs.delete(new Path(uri), true))
	    {
	    	System.out.println("Delete output dir first");
	    }
		
	    Job job = Job.getInstance(conf, "TurnningRate");
		
		job.setJarByClass(TurnningRateEntry.class);
		job.setMapperClass(TurnningRateMapper.class);
		job.setPartitionerClass(TurnningRatePartitioner.class);
		job.setReducerClass(TurnningRateReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setInputFormatClass(TextInputFormat.class);
		
		if (!job.waitForCompletion(true))
			return -1;
		
		return 0;
	}

}
