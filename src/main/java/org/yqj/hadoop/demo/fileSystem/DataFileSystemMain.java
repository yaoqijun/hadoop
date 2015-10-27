package org.yqj.hadoop.demo.fileSystem;

import org.apache.hadoop.fs.FileSystem;

/**
 * Created by yaoqijun.
 * Date:2015-10-27
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class DataFileSystemMain {

    private static final String FS_DEFAULT_NAME = "fs.default.name";

    private static final String FS_DEFAULT_VALUE = "hdfs://localhost:9000";

    public static void main(String []args){
        System.out.println("data file system main test");

        DataFileSystem dataFileSystem = new DataFileSystem();
        dataFileSystem.setConfiguration(FS_DEFAULT_NAME,FS_DEFAULT_VALUE);

        FileSystem fileSystem = dataFileSystem.getFileSystem();
        if(fileSystem == null){
            return;
        }
        DataHdfsSystemService dataHdfsSystemService = new DataHdfsSystemServiceImpl(fileSystem);

        //创建文件设置
//        try{
//            File file = new File("output.txt");
//            FileInputStream fileInputStream = new FileInputStream(file);
//            dataHdfsSystemService.createFile("/user/yaoqijun/localTest/output.txt",fileInputStream);
//        }catch (Exception e){
//            e.printStackTrace();
//            System.out.println("create file error");
//        }

        //下载读取文件内容
//        try{
//            FSDataInputStream fsDataInputStream = dataHdfsSystemService.dowloadFile("/user/yaoqijun/localTest/output.txt");
//            IOUtils.copyBytes(fsDataInputStream,System.out,1024,true);
//            System.out.println("finish");
//        }catch (Exception e){
//            System.out.println("download file error");
//        }

        //获取对应的文件的Status
//        try{
//            FileStatus fileStatus = dataHdfsSystemService.getFileStatus("/user/yaoqijun/localTest/output.txt");
//            System.out.println(fileStatus.getAccessTime());
//            System.out.println(fileStatus.getBlockSize());
//            System.out.println(fileStatus.getGroup());
//            System.out.println(fileStatus.getLen());
//            System.out.println(fileStatus.getModificationTime());
//            System.out.println(fileStatus.getOwner());
//            System.out.println(fileStatus.getReplication());
//            System.out.println(fileStatus.getSymlink());
//            System.out.println(fileStatus.getAccessTime());
//        }catch (Exception e){
//            System.out.println("get file status error");
//        }

        //测试删除内容
//        try{
//            //String dirPath = "/user/yaoqijun/localTest/testDir";
//            //Boolean result = dataHdfsSystemService.delete(dirPath);
//            //Boolean result = dataHdfsSystemService.delete(dirPath,true);   //exception throw is dir; dir not be delete
//            //System.out.println(result);
//
//            String filePath = "/user/yaoqijun/localTest/output.txt";
//            Boolean result = dataHdfsSystemService.delete(filePath, true);
//            System.out.println(result);
//        }catch (Exception e){
//            System.out.println("delete error");
//        }
        System.out.println("finish");
    }
}
