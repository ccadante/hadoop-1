import java.io.BufferedWriter;
import java.io.File;
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


public class SmartPVStatEntry extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		System.exit(ToolRunner.run(new SmartPVStatEntry(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		String input = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/smart_pv_stat/input/";
		String output = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/smart_pv_stat/output/";
		
		Configuration conf = getConf();
		
	    if (true)
	    {
	        conf.set("fs.defaultFS", "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020");
	        
	        conf.set("mapreduce.framework.name", "yarn");
	        conf.set("yarn.resourcemanager.address", "cp01-ma-eval-001.cp01.baidu.com:8032");	
	        conf.set("yarn.resourcemanager.resource-tracker.address", "cp01-ma-eval-001.cp01.baidu.com:8031");
	        conf.set("yarn.resourcemanager.scheduler.address", "cp01-ma-eval-001.cp01.baidu.com:8030");
	        
	        conf.set("mapreduce.jobhistory.address", "cp01-ma-eval-001.cp01.baidu.com:8044");
	        conf.set("mapreduce.jobhistory.webapp.address", "cp01-ma-eval-001.cp01.baidu.com:8045");
	        conf.set("mapreduce.jobhistory.done-dir", "/history/done");
	        conf.set("mapreduce.jobhistory.intermediate-done-dir", "/history/done_intermediate");

	        conf.set("mapreduce.app-submission.cross-platform", "true");
	        conf.set("mapreduce.job.am-access-disabled", "true");
	    }
	    
        File jarFile = EJob.createTempJar("bin");  
        EJob.addClasspath("/usr/hadoop/conf");  
        ClassLoader classLoader = EJob.getClassLoader();  
        Thread.currentThread().setContextClassLoader(classLoader);  
 
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
		
	    Job job = Job.getInstance(conf, "SmartPVStat");
	    job.setJar(jarFile.toString());
		job.setJarByClass(SmartPVStatEntry.class);
		job.setMapperClass(SmartPVStatMapper.class);
		job.setReducerClass(SmartPVStatReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(10);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		
		if (!job.waitForCompletion(true))
			return -1;
		
		
		
		return 0;
	}

}
