package dataexpo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//월별 운항거리 출력
public class DistanceWithDateKey{
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "DistanceWithDateKey");
		//입출력 데이터 경로 설정
		String in = "C:/ubuntu_share/dataexpo/1988.csv";
		String out = "outfile/distance-1988";
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(out))) {
			hdfs.delete(new Path(out), true);
			System.out.println("기존 출력파일 삭제");
		}
		job.setJarByClass(DistanceWithDateKey.class);
		job.setMapperClass(DistanceMapperWithDataKey.class);
		job.setReducerClass(DistanceReducerWithDataKey.class);
		job.setMapOutputKeyClass(DateKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
	}
}
