package org.yqj.hadoop.demo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by yaoqijun.
 * Date:2015-11-23
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class Dedup {

    private static final String FILE_INPUT_PATH = "input";

    private static final String FILE_OUTPUT_PATH = "output";

    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("the receive is key:"+key.get() +" value: "+ value.toString());
            IntWritable intWritable = new IntWritable(Integer.valueOf(value.toString()));
            context.write(new Text("FUCK"),intWritable);
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            for(IntWritable intWritable : values){
                if(intWritable.get() > max){
                    max = intWritable.get();
                }
            }
            context.write(new Text("MAX"),new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"dedup test");

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(FILE_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(FILE_OUTPUT_PATH));

        job.waitForCompletion(true);
        System.out.println("finish");
    }
}
