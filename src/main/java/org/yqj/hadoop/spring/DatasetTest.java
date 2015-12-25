package org.yqj.hadoop.spring;

import org.springframework.data.hadoop.store.DataStoreWriter;

/**
 * Created by yaoqijun.
 * Date:2015-10-28
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class DatasetTest {
    public static void main(String []args){
        System.out.println("data set test start");

        DatasetConfig datasetConfig = new DatasetConfig();

        try{
            DataStoreWriter<FileInfo> writer = datasetConfig.getDataStoreWriter();
            FileInfo fileInfo = new FileInfo("yaoqijun","path",1000l,1000l);
            writer.write(fileInfo);
            writer.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("spring write data error");
        }

    }
}
