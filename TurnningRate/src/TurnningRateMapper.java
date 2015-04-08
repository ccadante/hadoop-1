import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class TurnningRateMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Logger _logger = Logger.getLogger(TurnningRateMapper.class);
	private static final SimpleDateFormat _date_format =  new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
	
	//awk -F "(NOTICE: )|( xsp)|(q=)|(,city=)|(,ck=)|(,qid=)|(,pn=)|(,tn=)" '{print $2 "   " $4 "   " $6 "   " $7 "   " $8}'
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String log = value.toString().replaceAll("\\s", "");
		
		String[] inputs = log.split("(NOTICE:)|(xsp\\*)|(q=)|(,city=)|(,ck=)|(,qid=)|(,pn=)|(,tn=)");
		if (inputs.length < 8)
		{
			_logger.info(value.toString());
			context.getCounter("TurnningRate", "BAD_RECORD_FIELD_NUM").increment(1);
			return;
		}
		
		String time = inputs[1];
		String query = inputs[3].replaceAll("_", "");
		String baiduId = inputs[5];
		String qid = inputs[6];
		String pageNo = inputs[7];
		
		if (!query.isEmpty()  && !baiduId.isEmpty() && !time.isEmpty()  && !qid.isEmpty() && !pageNo.isEmpty())
		{
			long lTime = 0;
			try {
				lTime = (_date_format.parse(time)).getTime() / 1000;
			} catch (ParseException e) {
			}
			
			if (lTime != 0)
			{
				context.write(new Text(query + "_" + baiduId), new Text("" + lTime + "_" + qid + "_" + pageNo));
				return;
			}
		}
		else if (baiduId.isEmpty())
		{
			context.getCounter("TurnningRate", "EMPTY_COOKIE").increment(1);
		}
		else
		{
			_logger.info(value.toString());
			context.getCounter("TurnningRate", "BAD_RECORD_NUM").increment(1);
		}
	}

}
