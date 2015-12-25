package org.yqj.hadoop.demo.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by yaoqijun.
 * Date:2015-10-31
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class FileSystemDemo {

    private static final String host_name = "fs.default.name";

    private static final String host_url = "hdfs://localhost:9000";

    public static void main(String []args){
        try{
            FileSystem fileSystem = initConfiguration();

            //测试FileSystem
            //TestFileSystem(fileSystem);
            testDeleteFileSystem(fileSystem);
//            testCreateFileDir(fileSystem);
        }catch (Exception e){
            System.out.println("file system output error");
            e.printStackTrace();
        }
    }

    private static void testCreateFileDir(FileSystem fileSystem)throws Exception {
        Path path = new Path("/user/hive/warehouse/test");
        //System.out.println(fileSystem.exists(path));
        fileSystem.delete(path,true);
        System.out.println("finish");
    }

    private static void testDeleteFileSystem(FileSystem fileSystem)throws Exception{
        Path path = new Path("/user/yaoqijun/output");
        fileSystem.delete(path,true);
        System.out.println("over");
    }


    private static void TestFileSystem(FileSystem fileSystem) throws Exception{
        //测试文件状态
//        Path path = new Path("/user/yaoqijun");
//        FileStatus[] listStatus = fileSystem.listStatus(path);
//        for(FileStatus fileStatus : listStatus){
//            System.out.print(fileStatus.getPath().getName() + "     ");
//            System.out.print(fileStatus.getOwner());
//            System.out.println(fileStatus.getPath().getName());
//            System.out.println(fileStatus.getPath().getParent());
//        }

        //创建问题
//        Path path = new Path("/user/yaoqijun/interact.txt");
//        fileSystem.create(path,true);

        Path path = new Path("/user/yaoqijun/interact.txt");
        System.out.println(fileSystem.isDirectory(path));

    }

    private static FileSystem initConfiguration() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(configuration);
        return fs;
    }
}
