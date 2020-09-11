package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistanceMapperWithDataKey extends Mapper<LongWritable, Text, DateKey, LongWritable>{
	private final static LongWritable distance = new LongWritable();
	private DateKey outputKey = new DateKey();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		Airline al = new Airline(value);
		if(al.isDistanceAvailable()) {	
			if(al.getDistance() > 0 ) {	
				outputKey.setYear(al.getYear() + "");
				outputKey.setMonth(al.getMonth());
				distance.set(al.getDistance());	//거리 설정
				context.write(outputKey, distance);	//[1988,1: 100,1500,1000...]
			}
		}
	}
}
