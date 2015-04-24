import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;


public class BackSearchRateReducer extends Reducer<Text, NullWritable, NullWritable, NullWritable> {
	
	private String lastQuery = "";
	private ArrayList<String> _baiduIdList = new ArrayList<String>();
	private HTable _query_stat_table;
	private String _today_date;
	private String _pre_date;
	
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

        System.err.println("QUERY_STAT: " + _query_stat_table);
        _today_date = context.getConfiguration().get("CURRENT_DATE");
        /*
        Calendar calendar = Calendar.getInstance();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
        	calendar.setTime(sdf.parse(_today_date));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        calendar.add(Calendar.DAY_OF_MONTH, -1);
        _pre_date = sdf.format(calendar.getTime());
        */
	}
	
	private void writeToHBase() throws IOException
	{
		if (!lastQuery.isEmpty() && !_baiduIdList.isEmpty())
		{
			Put putToday = new Put(Bytes.toBytes(String.valueOf(lastQuery + "_" + _today_date)));
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			try {
				new ObjectOutputStream(out).writeObject(_baiduIdList);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return;
			}
			
			putToday.add(Bytes.toBytes("data"), Bytes.toBytes("baiduIdListToday"), out.toByteArray());

			try {
				_query_stat_table.put(putToday);
			} catch (RetriesExhaustedWithDetailsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedIOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void reduce(Text key, Iterable<NullWritable> values, Context context)
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
			_baiduIdList.add(baiduId);
		}
		else
		{
			if (_baiduIdList.isEmpty() || _baiduIdList.get(_baiduIdList.size() - 1) != baiduId)
			{
				_baiduIdList.add(baiduId);
			}
		}

		
	}
	
	protected void cleanup(Context context) throws IOException,
			InterruptedException 
	{
		writeToHBase();
	}
}
