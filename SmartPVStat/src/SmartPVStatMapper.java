import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SmartPVStatMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String _split_file_name;
	
	protected void setup(Context context) throws IOException,
			InterruptedException {
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		_split_file_name = inputSplit.getPath().getName();
		System.out.println("Current Spit: " + _split_file_name);
	}

	public void map(LongWritable ikey, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		if (_split_file_name.indexOf("query_raw.utf8.tab", 0) >= 0)
		{
			return;
		}
		else if (_split_file_name.indexOf("smart-se.log.wf", 0) >= 0)
		{
			if (line.indexOf("query prop display_field_info failed", 0) < 0)
			{
				System.out.println("This is Bad: " + value);
				return;
			}
			String [] splits = line.split("\\[|\\]");
			if (splits.length != 9)
			{
				System.out.println("Bad Field Count: " + value);
			}
			else
			{
				context.write(new Text(splits[7]), new Text("SE_WF\tDISPLAY_FIELD_FAIL"));
			}
		}
		else if (_split_file_name.indexOf("qas.log.wf", 0) >= 0) 
		{
			System.out.println("DEBUG: " + value);
			if (line.indexOf("get prop_name_value of prop_name", 0) < 0)
			{
				System.out.println("This is Bad: " + value);
				return;
			}
			String [] splits = line.split("\\]\\[qid:|\\] get prop_name_value");
			if (splits.length != 3)
			{
				System.out.println("Bad Field Count: " + value);
			}
			else
			{
				context.write(new Text(splits[1]), new Text("QAS_WF\tNO_PROP_NAME"));
			}
		}
		else if (_split_file_name.indexOf("smart-se.log", 0) >= 0)
		{
			if (line.indexOf("NOTICE:", 0) < 0 || line.indexOf("[src/worker.cpp][run]", 0) < 0)
			{
				System.out.println("Notice Bad: " + value);
				return;
			}
			
			String [] splits = line.split("Xsp_req:: qid|,query=|,trans_query=|\\] \\[Qas:: ret=|,time=|\\] \\[Xsp_res:: ret=");
	
			if (splits.length != 7)
			{
				System.out.println("Bad Field Count(qas may timeout): " + value);
				/*
				for (String split : splits)
				{
					System.out.println(split);
				}
				*/
			}
			else
			{
				String [] splits_0 = splits[0].split("\\[|\\]");
				if (splits_0.length != 9)
				{
					System.out.println("Bad Head: " + splits[0]);
				}
				else
				{
					//System.out.println("QID: " + splits_0[7]);
					//System.out.println("QUERY: " + splits[2]);
					//System.out.println("SE_RET: " + splits[6]);
					String seRet = "-1";
					String tplId = "-1";
					String qasRet = splits[4];
					if (splits[6].startsWith("0"))
					{
						//0,(src=27900,tid=3,tname=ecl_smartpl3,user=1,idea=1111614270),time:29] [Time:: 742]
						String [] splits_6 = splits[6].split(",tid=|,tname=");
						if (splits_6.length != 3)
						{
							System.out.println("BAD xsp ret: " + splits[6]);
							return;
						}
						else
						{
							seRet = "0";
							qasRet = "0";
							tplId = splits_6[1];
						}
					}
					
					context.write(new Text(splits_0[7]), new Text("SE_NOTICE\t" + splits[2] + "\t" + seRet + "\t" + tplId + "\t" + qasRet));
				}
			}
		}
		else //qas.log
		{
			if (line.indexOf("FunctionLexParse.cpp:66][", 0) < 0)
			{
				System.out.println("Bad Head: " + value);
				return;
			}
			
			String [] splits = line.split("FunctionLexParse.cpp:66\\]\\[qid:|\\] parse query.* fails, parse ret\\[");
			if (splits.length != 3)
			{
				System.out.println("Bad Field Count: " + value);
			}
			else
			{
				context.write(new Text(splits[1]), new Text("QAS_NOTICE_PARSE_RET\t" + splits[2].substring(0, 1)));
			}
		}
	}

}
