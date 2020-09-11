package wordcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCountEx2 {
	public static void main(String[] args) throws IOException{
		//Job + Configuration = JobConf
		JobConf conf = new JobConf(WordCountEx2.class);
		conf.setJobName("wordcount2");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		//맵퍼 클래스 지정
		conf.setMapperClass(Map.class);
		//컴바이너 클래스 지정
		conf.setCombinerClass(Reduce.class);
		//리듀서 클래스 지정
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("infile/in.txt"), new Path("infile/in2.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("outfile/wordcount"));
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path("outfile/wordcount"))) {
			hdfs.delete(new Path("outfile/wordcount"),true);
			System.out.println("기존 출력파일 삭제");
		}
		JobClient.runJob(conf);
	}
	
	public static class Map extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) 
						throws IOException{
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while(itr.hasMoreElements()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}//Map 내부 클래스 종료
	
	public static class Reduce extends MapReduceBase
		implements Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		public void reduce(Text key, Iterator<IntWritable> value, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {			
			int sum = 0;
			while(value.hasNext()) {
				sum += value.next().get();
			}
			output.collect(key, new IntWritable(sum));			
		}
	}// Reduce 내부 클래스 종료
}
