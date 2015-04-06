import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;


public class CTRStatEntry {

	public static final String CONF_KEY_CARS_FILE_PATH = "cars.data";
	
	public static void main(String[] args) throws Exception {
		
		String input = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/input/";
		String output = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/output/";
		String carsData = "hdfs://cp01-ma-eval-001.cp01.baidu.com:8020/weisai/conf_data/cars.data";
		
		Configuration conf = new Configuration();
		
		PropertyConfigurator.configure("log4j.properties");
		
		conf.set(CONF_KEY_CARS_FILE_PATH, carsData);
		
	    URI uri = new URI(output);
	    FileSystem fs = FileSystem.get(uri, conf, "work");
	    if (fs.delete(new Path(uri), true))
	    {
	    	System.out.println("Delete output dir first");
	    }
		
		Job job = Job.getInstance(conf, "CTRStat");
		job.setJarByClass(CTRStatEntry.class);
		// TODO: specify a mapper
		job.setMapperClass(CTRStatMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(CTRStatReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		if (!job.waitForCompletion(true))
			return;
	}

}
