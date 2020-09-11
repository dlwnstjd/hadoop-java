package dataexpo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWithMapSideJoin extends Mapper<LongWritable, Text, Text, Text>{
	private Hashtable<String, String> joinMap = new Hashtable<String, String>();
	private Text outkey = new Text();
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try {
			//cache 파일 정보 리턴
			URI[] cacheFiles = context.getCacheFiles();
			if(cacheFiles != null && cacheFiles.length > 0) {
				String line;
				//cacheFiles[0].toString(): cache 파일의 위치를 문자열로 리턴
				//							carriers.csv 파일 읽기
				BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				while((line = br.readLine()) != null) {
					CarrierCode code = new CarrierCode(line);
					//key: 항공사코드, value: 항공사이름
					joinMap.put(code.getCarrierCode(),  code.getCarrierName());
				}
				br.close();
			}else {
				System.out.println("cache file is null");
			}
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Airline al = new Airline(value);
		outkey.set(al.getUniqueCarrier());
		context.write(outkey, new Text(joinMap.get(al.getUniqueCarrier()) + "\t" + value.toString()));
	}
	
	
}
