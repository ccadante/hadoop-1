import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;


public class TurnningRateReducer extends Reducer<Text, Text, Text, Text> {
	
	class Pair {
		public int first;
		public int second;
	};
	
	private Map<String, Pair> _map = new HashMap<String, Pair>();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String[] key_inputs = key.toString().split("_");
		boolean hitPage0 = false;
		boolean hitPage1 = false;
		
		long minTime = Long.MAX_VALUE;
		long maxTime = 0;
		
		for (Text val : values) {
			String[] value_inputs = val.toString().split("_");
			String pageNo = value_inputs[2];

			System.out.println(key_inputs[0] + "\t" + key_inputs[1] + "\t" + value_inputs[0] + "\t" + value_inputs[1] + "\t" + pageNo);
			
			if (pageNo.equals("0") )
			{
				hitPage0 = true;
			}
			else if (pageNo.equals("1") )
			{
				hitPage1 = true;
			}
			else
			{
				context.getCounter("TurnningRate", "BAD_PAGE_NO").increment(1);
				continue;
			}
			
			long time = Long.parseLong(value_inputs[0]);
			minTime = Math.min(minTime, time);
			maxTime = Math.max(maxTime, time);
			
			if (maxTime - minTime >= 300)
			{
				context.getCounter("TurnningRate", "OVER_TIME_COUNT").increment(1);
				System.out.println(maxTime - minTime);
			}
		}
		
		Pair newEntry = null;
		if (_map.containsKey(key_inputs[0]))
		{
			newEntry = _map.get(key_inputs[0]);
		}
		else
		{
			newEntry = new Pair();
		}
		
		if (hitPage0 || hitPage1)
		{
			newEntry.first += 1;
		}
		if (hitPage0 && hitPage1)
		{
			newEntry.second += 1;
		}
		
		_map.put(key_inputs[0], newEntry);
	}
	
	protected void cleanup(Context context) throws IOException,
			InterruptedException 
	{
		for (Entry<String, Pair> entry : _map.entrySet())
		{
			String query = entry.getKey();
			Pair pair = entry.getValue();
			
			context.write(new Text(query), new Text("" + pair.first + "\t" + pair.second));
		}
	}
}
