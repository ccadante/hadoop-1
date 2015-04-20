import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;


public class BackSearchRateReducer extends Reducer<Text, NullWritable, NullWritable, NullWritable> {
	
	private String lastQuery = "";
	private LinkedList<String> _baiduIdList = new LinkedList<String>();
	private HTable _query_stat_table;
	private String _date_colume_name;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = new Configuration();
		configuration = HBaseConfiguration.addHbaseResources(configuration);
		
		configuration.set("hbase.zookeeper.quorum", "cp01-ma-eval-001.cp01.baidu.com");
		configuration.set("hbase.zookeeper.property.clientPort", "8071");
		configuration.set("hbase.master.port", "8070");
		configuration.set("hbase.regionserver.port", "8072");
		
        try {
        	_query_stat_table = new HTable(configuration, "query_stat");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        _date_colume_name = context.ge
	}
	
	private void writeToHBase()
	{
		if (!lastQuery.isEmpty() && !_baiduIdList.isEmpty())
		{
			Put p = new Put(Bytes.toBytes(String.valueOf(lastQuery)));
			
			p.add(Bytes.toBytes("cookies"), Bytes.toBytes(_date_colume_name), Bytes.toBytes(sentence));
			try {
				_query_stat_table.put(p);
			} catch (RetriesExhaustedWithDetailsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedIOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String[] key_inputs = key.toString().split("\t");
		String query = key_inputs[0];
		String baiduId = key_inputs[1];
		
		if (_baiduIdList.isEmpty())
		{
			lastQuery = query;
		}
	
		if (!lastQuery.equals(query))
		{
			//将之前的内容写入HBASE
			writeToHBase();

			lastQuery = query;
			_baiduIdList.clear();
			_baiduIdList.offer(baiduId);
		}
		else
		{
			if (_baiduIdList.isEmpty() || _baiduIdList.getLast() != baiduId)
			{
				_baiduIdList.offer(baiduId);
			}
		}

		
	}
	
	protected void cleanup(Context context) throws IOException,
			InterruptedException 
	{
		writeToHBase();
	}
}
