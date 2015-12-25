package org.yqj.hadoop.demo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yaoqijun.
 * Date:2015-11-23
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class Sort {

    private static final String FILE_INPUT_PATH = "input";

    private static final String FILE_OUTPUT_PATH = "output";

    public static class Map extends Mapper<LongWritable,Text,LongWritable,LongWritable>{
        private static LongWritable data = new LongWritable();

        @Override
        protected void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {
            data = new LongWritable(Long.valueOf(value.toString()));
            context.write(data, new LongWritable(1));
        }
    }

    public static class Reduce extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{

        private static LongWritable lineNum = new LongWritable(1);

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for(LongWritable longWritable : values){
                context.write(key,longWritable);
                lineNum = new LongWritable(lineNum.get() + 1);
            }
        }
    }

    public static class Partition extends Partitioner<LongWritable,LongWritable>{

        @Override
        public int getPartition(LongWritable intWritable, LongWritable intWritable2, int numPartitions) {
            int max = 65223;
            int bound = max/numPartitions + 1;
            long keyNumber = intWritable.get();
            for(int i=0; i<numPartitions; i++){
                if(keyNumber<bound*(i+1)&&keyNumber>=bound*i){
                    return i;
                }
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Sort file int");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"Sort");
        //job.setJarByClass(Sort.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(FILE_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(FILE_OUTPUT_PATH));
        System.out.println(job.waitForCompletion(true));
    }
}
