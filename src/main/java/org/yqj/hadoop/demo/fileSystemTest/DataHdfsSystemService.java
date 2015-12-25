package org.yqj.hadoop.demo.fileSystemTest;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.FileInputStream;

/**
 * Created by yaoqijun.
 * Date:2015-10-27
 * Email:yaoqj@terminus.io
 * Descirbe:    hadoop FileSystem 相关操作
 */
public interface DataHdfsSystemService {

    /**
     * 删除对应的文件
     * @param path  文件路径
     * @param recursive 删除目录 && recursive:true 删除。
     * @return
     */
    Boolean delete(String path,boolean recursive);

    /**
     * 获取对应的文件内容
     * @param path
     * @return
     */
    FSDataInputStream dowloadFile(String path);

    /**
     * 创建文件
     * @param path
     * @param fileInputStream
     * @return
     */
    Boolean createFile(String path, FileInputStream fileInputStream);

    /**
     * 获取对应的文件Status属性
     * @param path
     * @return
     */
    FileStatus getFileStatus(String path);

    /**
     * 退出是删除对应的文件
     * @param path
     * @return
     */
    Boolean deleteOnExist(String path);
}
