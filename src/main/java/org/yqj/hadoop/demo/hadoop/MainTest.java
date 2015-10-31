package org.yqj.hadoop.demo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.output.TextFileWriter;

/**
 * Created by yaoqijun.
 * Date:2015-10-28
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class MainTest {
    public static void main(String []args){
        System.out.println("main test result");
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name","hdfs://localhost:9000");
            Path path = new Path("/user/spring/again.txt");

            TextFileWriter textFileWriter = new TextFileWriter(configuration,path,null);
            textFileWriter.write("再次添加数据内容");
            System.out.println("finish");

            //文件的读写操作
            //TextSequenceFileWriter textSequenceFileWriter = new TextSequenceFileWriter();

            //DelimitedTextFileWriter delimitedTextFileWriter = new DelimitedTextFileWriter();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
