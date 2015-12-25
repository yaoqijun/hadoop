package org.yqj.hadoop.demo.fileSystemTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * Created by yaoqijun.
 * Date:2015-10-27
 * Email:yaoqj@terminus.io
 * Descirbe:    data 对应的FileSystem 连接
 */
public class DataFileSystem {

    private Configuration configuration;

    private FileSystem fileSystem;

    public DataFileSystem(){
        configuration = new Configuration();
    }

    /**
     * 创建FileSystem
     * @param url
     * @return
     */
    public FileSystem getFileSystem(String url){
        try{
            return FileSystem.get(new URI(url),configuration);
        }catch (Exception e){
            return null;
        }
    }

    /**
     * 获取FileSystem
     * @return
     * @throws Exception
     */
    public FileSystem getFileSystem(){
        try{
            return FileSystem.get(configuration);
        }catch (Exception e){
            return null;
        }
    }

    /**
     * 设置配置属性
     * @param name  属性名称
     * @param value 属性值
     */
    public void setConfiguration(String name, String value){
        configuration.set(name,value);
    }
}
