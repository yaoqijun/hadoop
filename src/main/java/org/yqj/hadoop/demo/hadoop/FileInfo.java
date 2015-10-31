package org.yqj.hadoop.demo.hadoop;

/**
 * Created by yaoqijun.
 * Date:2015-10-28
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class FileInfo {
    private String name;
    private String path;
    private long size;
    private long modified;

    public FileInfo(String name, String path, long size, long modified) {
        this.name = name;
        this.path = path;
        this.size = size;
        this.modified = modified;
    }

    public FileInfo() {
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
    }

    public long getModified() {
        return modified;
    }
}
