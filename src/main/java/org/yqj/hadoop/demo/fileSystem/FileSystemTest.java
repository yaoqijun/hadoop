package org.yqj.hadoop.demo.fileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;

/**
 * Created by yaoqijun.
 * Date:2015-10-27
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class FileSystemTest {
    public static void main(String []args){
        System.out.println("yaoqijun");
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name","hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(configuration);
            InputStream inputStream = fs.open(new Path("/user/yaoqijun/employee/part-m-00000"));
            IOUtils.copyBytes(inputStream,System.out,4096,false);
            IOUtils.closeStream(inputStream);
        }catch (Exception e){
            System.out.println("file system output error");
            e.printStackTrace();
        }
    }
}