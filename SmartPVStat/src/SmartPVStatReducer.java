import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SmartPVStatReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String query = null;
		int seRet = -1;
		int tplId = -1;
		int qasRet = -1;
		int parseRet = -1;
		boolean no_entity = true;
		boolean not_any_property = false;
		
		for (Text val : values) {
			String[] inputs = val.toString().split("\t");
			if (inputs.length == 0)
			{
				System.out.println("BAD QID: " + key);
				return;
			}
			
			String cmd = inputs[0];
			if (cmd.equals("SE_NOTICE"))
			{
				query = inputs[1];
				seRet = Integer.parseInt(inputs[2]);
				tplId = Integer.parseInt(inputs[3]);
				qasRet = Integer.parseInt(inputs[4]);
			}
			else if (cmd.equals("SE_WF"))
			{
				if (inputs[1].equals("DISPLAY_FIELD_FAIL"))
				{
					no_entity = false;
				}
				else
				{
					System.out.println("BAD OUTPUT for SE_WF: " + key);
				}
			}
			else if (cmd.equals("QAS_WF"))
			{
				if (inputs[1].equals("NO_PROP_NAME"))
				{
					not_any_property = true;
				}
				else
				{
					System.out.println("BAD OUTPUT for QAS_WF: " + key);
				}
			}
			else if (cmd.equals("QAS_NOTICE_PARSE_RET"))
			{	
				parseRet = Integer.parseInt(inputs[1]);
			}
		}
		
		if (query != null)
		{
			if (seRet == 0)
			{
				context.write(new Text(query), new Text("TPL_" + tplId));
			}
			else if (qasRet == 0)
			{
				if (no_entity)
				{
					context.write(new Text(query), new Text("NO_ENTITY"));
				}
				else
				{
					context.write(new Text(query), new Text("PROPERTY_QUERY_FAIL"));
				}
			}
			else if (parseRet == -1)
			{
				if (not_any_property == true)
				{
					context.write(new Text(query), new Text("NOT_ANY_PROPERTY"));
				}
				else 
				{
					context.write(new Text(query), new Text("UNEXPECTED_MINUS_1"));
				}
			}
			else if (parseRet == 3)
			{
				if (not_any_property == true)
				{
					context.write(new Text(query), new Text("NOT_ANY_PROPERTY"));
				}
				else 
				{
					context.write(new Text(query), new Text("UNEXPECTED_3"));
				}
			}
			else
			{
				context.write(new Text(query), new Text("PARSE_" + parseRet));
			}
		}
		else
		{
			System.out.println("NO SE_NOTICE LOG for KEY: " + key);
		}
	}
}
