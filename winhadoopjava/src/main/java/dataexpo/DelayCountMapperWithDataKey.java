package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DelayCountMapperWithDataKey extends Mapper<LongWritable, Text, DateKey, IntWritable>{
	//map 출력값
	private final static IntWritable one = new IntWritable(1);
	//map 출력키
	private DateKey outputKey = new DateKey();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		Airline al = new Airline(value);
		//출발 지연 데이터 출력
		if(al.isDepartureDelayAvailable()) {	//출발 지연 대상인 비행기
			if(al.getDepartureDelayTime() > 0 ) {	//지연 출발 비행기
				//출력키 설정
				outputKey.setYear("D," + al.getYear());
				outputKey.setMonth(al.getMonth());
				//출력 데이터 생성
				context.write(outputKey, one);	//"1998,1",1,1,1,1....
			}else if(al.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			}else if(al.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
		}else {//출발 지연 대상이 아닌 비행기
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}
		//도착 지연 데이터 출력
		if(al.isArriveDelayAvailable()) {	//도착 지연 대상인 비행기
			if(al.getArriveDelayTime() > 0) {
				outputKey.setYear("A," + al.getYear());
				outputKey.setMonth(al.getMonth());
				context.write(outputKey, one);
			}else if(al.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			}else if(al.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		}else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}
	}
	
}
