package org.yqj.hadoop.demo;

import org.yqj.hadoop.demo.hadoop.HdfsFileService;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HdfsFileServiceMain {
    public static void main(String []args) throws Exception{
        System.out.println("yaoqijun");
        HdfsFileService service = new HdfsFileService();
        File file = new File("output.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);

        InputStream inputStream = service.downloadFile("/user/yaoqijun/teacher/part-m-00000");
        byte[] buffer = new byte[1024];
        int byteRead = 0;
        byteRead = inputStream.read(buffer,0,1024);
        fileOutputStream.write(buffer);
        inputStream.close();
        fileOutputStream.close();
    }
}
