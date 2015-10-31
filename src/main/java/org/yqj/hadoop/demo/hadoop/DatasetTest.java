package org.yqj.hadoop.demo.hadoop;

import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.dataset.DatasetOperations;

import java.io.File;

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
        DatasetOperations datasetOperations = datasetConfig.datasetOperations();
        DataStoreWriter<FileInfo> writer = datasetConfig.dataStoreWriter();

        //upload file content
        File file = new File("output.txt");
        try{
            FileInfo fileInfo = new FileInfo(file.getName(),file.getParent(),file.length(),file.lastModified());
            writer.write(fileInfo);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                writer.close();
            } catch (Exception e) {
                System.out.println("close fail");
            }
        }
    }
}
