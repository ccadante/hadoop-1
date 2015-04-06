import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TopNQueryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private PriorityQueue<Map.Entry<String, Integer>> _queue = null;
	
	private Map<String, Integer> _map = new HashMap<String, Integer>();
	
	private int _top_num = 0;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		Configuration conf = context.getConfiguration();
		_top_num = conf.getInt(TopNQueryEntry.CONF_TOP_NUM_KEY_NAME, TopNQueryEntry.DEFAULT_TOP_NUM);
		_queue = new PriorityQueue<Map.Entry<String, Integer>>(
				_top_num, new Comparator<Map.Entry<String, Integer>>(){
					public int compare(Entry<String, Integer> o1,
							Entry<String, Integer> o2) {
						return o1.getValue() - o2.getValue();
					}
				});
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {	
		
		String query = value.toString();
		if (query.isEmpty())
		{
			return;
		}
		
		System.out.println(query);
		if (_map.containsKey(query))
		{
			Integer count = _map.get(query);
			count++;
			_map.put(query, count);
		}
		else
		{
			_map.put(query, 1);
		}
	}
	
	  protected void cleanup(Context context
	                         ) throws IOException, InterruptedException {
	   
		  Iterator<Entry<String, Integer>> iterator = _map.entrySet().iterator();
		  
		  while (iterator.hasNext())
		  {
			  Map.Entry<String, Integer> entity = iterator.next();
			  
			  if (_queue.size() < _top_num)
			  {
				  _queue.add(entity);
			  }
			  else
			  {
				  _queue.remove();
				  _queue.add(entity);
			  }
		  }
		  
		  iterator = _queue.iterator();
		  while (iterator.hasNext())
		  {
			  Map.Entry<String, Integer> entity = iterator.next();
			  context.write(new Text(entity.getKey()), new IntWritable(entity.getValue()));
		  }
	  }
}
