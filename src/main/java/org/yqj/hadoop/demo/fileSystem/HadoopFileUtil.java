package org.yqj.hadoop.demo.fileSystem;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.fs.FileSystemFactoryBean;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HadoopFileUtil {
    public static void main(String[] args){

        System.out.println("hadoop file util");

        try{
            AbstractApplicationContext context = new ClassPathXmlApplicationContext("hadoop-context.xml");
            FileSystemFactoryBean fileSystemFactoryBean =(FileSystemFactoryBean)context.getBean("hadoopFs");
            FileSystem fileSystem = fileSystemFactoryBean.getObject();
            FSDataInputStream out = null;
            Path hdfsPath =new Path("/user/yaoqijun/employee/part-m-00000");
            try {
                out = fileSystem.open(hdfsPath);
            } catch (Exception e) {
                e.printStackTrace();
            }
            byte[] buffer = new byte[1024];
            int byteCount = 0;
            while ((byteCount = out.read(buffer,0,1024))!=-1){
                System.out.println(buffer);
            }
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("hadoop file error");
        }

    }
}
