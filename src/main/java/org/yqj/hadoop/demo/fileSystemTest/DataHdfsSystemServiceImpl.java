package org.yqj.hadoop.demo.fileSystemTest;

import com.google.common.base.Strings;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;

/**
 * Created by yaoqijun.
 * Date:2015-10-27
 * Email:yaoqj@terminus.io
 * Descirbe:    对文件系统的操作
 */
public class DataHdfsSystemServiceImpl implements DataHdfsSystemService {

    private static final Integer BUFFER_SIZE = 2048;    //创建文件拷贝文件块大小

    private FileSystem fileSystem;

    public DataHdfsSystemServiceImpl(FileSystem system){
        this.fileSystem = system;
    }

    @Override
    public Boolean delete(String path,boolean recursive) {

        if(Strings.isNullOrEmpty(path)){
            return Boolean.FALSE;
        }

        Path hdfsPath = new Path(path);

        try{
            fileSystem.delete(hdfsPath,recursive);
            return Boolean.TRUE;
        }catch (Exception e){
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    @Override
    public FSDataInputStream dowloadFile(String path) {
        FSDataInputStream dataInputStream = null;

        if(Strings.isNullOrEmpty(path)){
            return dataInputStream;
        }

        Path hdfsPath = new Path(path);

        try{
            dataInputStream = fileSystem.open(hdfsPath);
            return dataInputStream;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Boolean createFile(String path, FileInputStream fileInputStream) {

        if(Strings.isNullOrEmpty(path)){
            return Boolean.FALSE;
        }

        Path hdfsPath = new Path(path);

        try{
            FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsPath);
            IOUtils.copyBytes(fileInputStream,fsDataOutputStream,BUFFER_SIZE,Boolean.TRUE);
            return Boolean.TRUE;
        }catch (Exception e){
            e.printStackTrace();
            return Boolean.TRUE;
        }
    }

    @Override
    public FileStatus getFileStatus(String path) {

        //validate
        if(Strings.isNullOrEmpty(path)){
            return null;
        }

        Path hdfsPath = new Path(path);
        try{
            FileStatus fileStatus = fileSystem.getFileStatus(hdfsPath);
            return fileStatus;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Boolean deleteOnExist(String path) {

        if (Strings.isNullOrEmpty(path)){
            return Boolean.TRUE;
        }

        Path hdfsPath = new Path(path);
        try{
            fileSystem.deleteOnExit(hdfsPath);
            return Boolean.TRUE;
        }catch (Exception e){
            return Boolean.FALSE;
        }
    }
}
