package org.yqj.hadoop.demo.mapreduce;

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

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by yaoqijun.
 * Date:2015-11-23
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class WordCount {

    private static final String FILE_INPUT_PATH = "input";

    private static final String FILE_OUTPUT_PATH = "output";

    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreElements()){
                word.set(tokenizer.nextToken());
                outputCollector.collect(word,one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()){
                sum += iterator.next().get();
            }
            outputCollector.collect(text,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println("hadoop 单词数量统计");

        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        //设置数据来源的格式， 数据去向的格式
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(FILE_INPUT_PATH));
        FileOutputFormat.setOutputPath(conf, new Path(FILE_OUTPUT_PATH));

        JobClient.runJob(conf);

    }
}
