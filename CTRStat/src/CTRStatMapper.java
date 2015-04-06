import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/*

cars.txt
car1,car2,car3

PV_aladdin.txt
query,click_num,show_num,ctr

PV_pl.txt

*/

public class CTRStatMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

	private Set<String> set = new HashSet<String>();

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String carsFile = conf.get(CTRStatEntry.CONF_KEY_CARS_FILE_PATH);
		
		if (carsFile == null || carsFile.isEmpty())
		{
			throw new RuntimeException("Invalid cars.data config");
		}
		
		Path carsFilePath = new Path(carsFile);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(carsFile), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		//以下为按行读取文件
		//BufferedReader lineReader = new BufferedReader(new InputStreamReader(fs.open(carsFilePath), "GBK"));
		
		InputStream carsStream = fs.open(carsFilePath);
		FileStatus fileStatus = fs.getFileStatus(carsFilePath);
		int fileSize = (int) fileStatus.getLen();
		byte[] buf = new byte[fileSize];
		IOUtils.readFully(carsStream, buf, 0, fileSize);
		String content = new String(buf);
		String[] cars = content.split(",");
		for (String car : cars)
		{
			set.add(car.toLowerCase(Locale.ENGLISH));
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("KEY: " + key + ",Value: " + value);
		String[] inputs = value.toString().split(",");
		if (inputs.length != 4)
		{
			throw new RuntimeException("Invalid input format");
		}
		String query = inputs[0];
		
		boolean found = false;
		Iterator<String> iterator = set.iterator();
		while (iterator.hasNext())
		{
			String car = iterator.next();
			if (-1 != query.toLowerCase(Locale.ENGLISH).indexOf(car))
			{
				IntArrayWritable output_value = new IntArrayWritable(new String[]{inputs[1], inputs[2]});
				context.write(new Text(car), output_value);	
				found = true;
			}
		}
		
		if (!found)
		{
			System.out.println("Bad Query, not contained in CARS");
		}
	}

}
