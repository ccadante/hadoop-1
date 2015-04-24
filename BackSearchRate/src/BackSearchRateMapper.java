import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


public class BackSearchRateMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private static final Logger _logger = Logger.getLogger(BackSearchRateMapper.class);

	
	protected void setup(Context context
              ) throws IOException, InterruptedException {
	}
	  
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] inputs = value.toString().split("\t");
		if (inputs.length != 5)
		{
			_logger.info(value.toString());
			context.getCounter("BackSearchRate", "BAD_RECORD_FIELD_NUM").increment(1);
			return;
		}
		
		String time = inputs[0];
		String query = inputs[2];
		String baiduId = inputs[3];
		
		if (!query.isEmpty()  && !baiduId.isEmpty() && !time.isEmpty())
		{
			context.write(new Text(query + "\t" + baiduId), NullWritable.get());
		}
		else if (baiduId.isEmpty())
		{
			context.getCounter("BackSearchRate", "EMPTY_COOKIE").increment(1);
		}
		else
		{
			_logger.info(value.toString());
			context.getCounter("BackSearchRate", "BAD_RECORD_NUM").increment(1);
		}
	}

}
