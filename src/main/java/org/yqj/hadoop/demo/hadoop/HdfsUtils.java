package org.yqj.hadoop.demo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HdfsUtils {

    private static Configuration conf = new Configuration();

    private static FileSystem fs = null;

    /**
     * 获取Hadoop的Configuration
     * @return
     */
    public static synchronized Configuration getConfiguration() {
        return conf;
    }

    /**
     * 该方法只会执行一次，即在第一次创建FileSystem时
     * @throws IOException
     * @throws URISyntaxException
     */
    private synchronized static void initFileSystem(String url) throws Exception{
        if(fs ==null)
            fs = FileSystem.get(new URI(url),conf);
    }

    public static FileSystem getFileSystem(String url) throws Exception{
        if(fs ==  null)//是第一次创建
            initFileSystem(url);
        return fs;
    }

    public synchronized static String convertSize(long size) {
        String result = String.valueOf(size);
        if (size < 1024 * 1024) {
            result = String.valueOf(size / 1024) + " KB";
        } else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
            result = String.valueOf(size / 1024 / 1024) + " MB";
        } else if (size >= 1024 * 1024 * 1024) {
            result = String.valueOf(size / 1024 / 1024 / 1024) + " GB";
        } else {
            result = result + " B";
        }
        return result;
    }
}
