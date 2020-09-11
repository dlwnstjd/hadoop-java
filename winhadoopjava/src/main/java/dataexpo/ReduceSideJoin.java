package dataexpo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ReduceSideJoin {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String[] arg = {
				"C:/ubuntu_share/dataexpo/carriers.csv",
				"C:/ubuntu_share/dataexpo/1988.csv",
				"outfile/reducesidejoin"
		};
		Configuration conf = new Configuration();
		Job job = new Job(conf, "reducesidejoin");
		FileOutputFormat.setOutputPath(job, new Path(arg[2]));
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(arg[2]))) {
			System.out.println("기존 출력파일 삭제");
			hdfs.delete(new Path(arg[2]));
		}
		
		job.setJarByClass(ReduceSideJoin.class);
		job.setPartitionerClass(TaggedGroupKeyPartitioner.class);
		job.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
		job.setSortComparatorClass(TaggedGroupComparator.class);
		
		job.setReducerClass(ReducerWithReduceSideJoin.class);
		job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//다중 입력 파일
		MultipleInputs.addInputPath(job, new Path(arg[0]),	//항공사코드 
				TextInputFormat.class, CarrierCodeMapper.class);
		MultipleInputs.addInputPath(job, new Path(arg[1]), 	//비행정보
				TextInputFormat.class, MapperWithReduceSideJoin.class);
		job.waitForCompletion(true);
		
	}
}
