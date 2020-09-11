package dataexpo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCountWithDateKey extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		String arg[] = {"-D","workType=departure", "C:/ubuntu_share/dataexpo/1988.csv","outfile/depart-1988"};
		int res = ToolRunner.run(new Configuration(), new DelayCountWithDateKey(), arg);
	}
	@Override
	public int run(String[] args) throws Exception{
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.out.println("Usage: DelayCountWithDateKey <in> <out>");
			System.exit(2);
		}
		Job job = new Job(getConf(), "DelayCountWithDateKey");
		//입출력 데이터 경로 설정
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//기존 파일 삭제하기
		FileSystem hdfs = FileSystem.get(getConf());
		if(hdfs.exists(new Path(otherArgs[1]))) {
			hdfs.delete(new Path(otherArgs[1]), true);
			System.out.println("기존 출력파일 삭제");
		}
		
		//Job 클래스 설정
		job.setJarByClass(DelayCountWithDateKey.class);
		//Mapper 클래스 설정
		job.setMapperClass(DelayCountMapperWithDataKey.class);
		//Reducer 클래스 설정
		job.setReducerClass(DelayCountReducerWithDataKey.class);
		//키를 설정: WritableComparable 인터페이스를 구현해야함
		job.setMapOutputKeyClass(DateKey.class);
		//value 설정: Writable 인터페이스를 구현 해야함
		job.setMapOutputValueClass(IntWritable.class);
		//입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//출력키 및 출력값 유형 설정
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);
		//MultipleOutputs 설정
		//출발 지연 건수 정보
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, DateKey.class, IntWritable.class);
		//도착 지연 건수 정보
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, DateKey.class, IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}
}
