package org.yqj.hadoop.demo.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Created by yaoqijun.
 * Date:2015-11-25
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class SingleTable {

    private static final String FILE_INPUT_PATH = "input";

    private static final String FILE_OUTPUT_PATH = "output";

    public static class Map extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parentChild = value.toString().split(" ");
            String child = parentChild[0];
            String parent = parentChild[1];
            //  设置 1-child : child parent 2-parent : child parent
            context.write(new Text(child),new Text("1" + "-" +child + "-" + parent));
            context.write(new Text(parent), new Text("2" + "-" + child + "-" + parent));
            //System.out.println("map get context "+key.toString() + " "+ value.toString());
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> parents = Lists.newArrayList(); //父亲们
            List<String> childs = Lists.newArrayList(); //孩子们

            values.forEach(value -> {
                String valueStr = value.toString();
                String[] text = valueStr.split("-");
                if (text[0].equals("1")) {
                    parents.add(text[2]);
                } else if (text[0].equals("2")) {
                    childs.add(text[1]);
                }
            });

            System.out.println("*********************************************");
            System.out.println("text : "+ childs.toString());
            System.out.println("text : "+parents.toString());
            System.out.println("*********************************************");

            //输出对应的笛卡尔积
            childs.forEach(child -> {
                parents.forEach(parent -> {
                    try {
                        context.write(new Text(child), new Text(parent));
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("error write context");
                    }
                });
            });

        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println("start table content contract");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"table relative");
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(FILE_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(FILE_OUTPUT_PATH));

        job.waitForCompletion(true);
        System.out.println("finish the job content");

    }

}
