import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class NetLogCollectEntry {
	
	static public class DataRecvJob implements Runnable {
		DatagramSocket _listen_socket = null;
		BlockingQueue _dataQueue = null;

		public DataRecvJob(int port, BlockingQueue queue)
				throws SocketException {
			_listen_socket = new DatagramSocket(port);
			_dataQueue = queue;
		}

		public void run() {
			while (true) {
				byte[] buffer = new byte[10240];
				DatagramPacket p = new DatagramPacket(buffer, buffer.length);
				try {
					_listen_socket.receive(p);
				} catch (IOException e) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

				System.out.println("RECV: " + p.getLength() + "Bytes");

				if (p.getLength() >= buffer.length) {
					continue;
				}

				_dataQueue.offer(p.getData());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		Configuration configuration = new Configuration();
		configuration = HBaseConfiguration.addHbaseResources(configuration);
		configuration.set("hbase.zookeeper.quorum", "cp01-ma-eval-001.cp01.baidu.com");
		configuration.set("hbase.zookeeper.property.clientPort", "8071");
		configuration.set("hbase.master.port", "8070");
		configuration.set("hbase.regionserver.port", "8072");

        HBaseAdmin admin = new HBaseAdmin(configuration);
        if (admin.tableExists("xsp_log"))
        {
        	System.out.println("表已经存在！");
        	return;
        }
        
		BlockingQueue dataQueue = new LinkedBlockingQueue<byte[]>();
		Thread dataRecvThread = null;
		
		try {
			dataRecvThread = new Thread(new DataRecvJob(12345, dataQueue));
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dataRecvThread.start();

		while (true)
		{
			byte[] data = null;
			try {
				data = (byte[]) dataQueue.poll(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (data == null) {
				continue;
			}

			String str = new String(data);
			System.out.println(str);	
		}
	}
}