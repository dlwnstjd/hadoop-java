package reducetest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWithReduceSideJoin 
	extends Mapper<LongWritable, Text, TaggedKey, Text>{
	TaggedKey outputkey = new TaggedKey();

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//value(비행정보): 년도, 월, 일, 항공사코드 ...
		Airline al = new Airline(value);
		outputkey.setCarrierCode(al.getUniqueCarrier());
		outputkey.setTag(al.getMonth());
		context.write(outputkey, value);
	}
	
}
