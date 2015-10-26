package org.yqj.hadoop.demo.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HdfsFileService{

    protected FileSystem fs;

    public void deleteFile(String path) {
        Path hdfsPath =new Path( path);
        try {
            getFs().delete(hdfsPath, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public InputStream downloadFile(String path) throws Exception {
        FSDataInputStream out = null;
        Path hdfsPath =new Path(path);
        try {
            out = getFs().open(hdfsPath);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return out;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public FileSystem getFs() {
        if(fs!=null) return fs;
        try {
            fs = HdfsUtils.getFileSystem("hdfs://localhost:9000");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fs;
    }

}
