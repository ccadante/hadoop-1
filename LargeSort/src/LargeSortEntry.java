import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;


public class LargeSortEntry extends Configured implements Tool {

	public static final String CONF_TOP_NUM_KEY_NAME = "topNum";
	
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		System.exit(ToolRunner.run(new LargeSortEntry(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		String input = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/large_sort/input/";
		String output = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/large_sort/output/";
		
		Configuration conf = getConf();
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(input), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		/*
		Random randomGenerator = new Random();
		Path inputPath = new Path(input +"input.dat");
		BufferedWriter lineWriter = new BufferedWriter(new OutputStreamWriter(fs.create(inputPath), "GBK"));
		
		int total = 10000000;
		int percent = total / 100;
		for (int i = 0; i < total; ++i)
		{
			long r = (-1L) * Integer.MIN_VALUE  + randomGenerator.nextInt();
			lineWriter.write((String.valueOf(r) + "\n"));
			if (i % percent == 0)
			{
				System.out.println("" + (float)i / total * 100 + "%");
			}
		}
		
		lineWriter.close();
		*/
		
	    URI uri = new URI(output);
	    if (fs.delete(new Path(uri), true))
	    {
	    	System.out.println("Delete output dir first");
	    }
		
		Job job = Job.getInstance(conf, "LargeSort");

		job.setJarByClass(LargeSortEntry.class);
		job.setMapperClass(LargeSortMapper.class);
		
		job.setReducerClass(LargeSortReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(10);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		JobClient jc = null;
		job.getStatus();
		
		//作业完成回调URL， job.end.notification.url
		
		//采样
		InputSampler.RandomSampler<LongWritable, Text> sampler = 
				new InputSampler.RandomSampler<LongWritable, Text>(0.1f, 100000);
		Path partitionFile = new Path(input, "_partitions");
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		InputSampler.writePartitionFile(job, sampler);
		URI partitionURI = new URI(partitionFile.toString() + "#_partitions");
		DistributedCache.addCacheFile(partitionURI, conf);
		DistributedCache.createSymlink(conf);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		if (!job.waitForCompletion(true))
			return -1;
		
		return 0;
	}

}
