package org.yqj.hadoop.demo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.Formats;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.dataset.AvroPojoDatasetStoreWriter;
import org.springframework.data.hadoop.store.dataset.DatasetDefinition;
import org.springframework.data.hadoop.store.dataset.DatasetOperations;
import org.springframework.data.hadoop.store.dataset.DatasetRepositoryFactory;
import org.springframework.data.hadoop.store.dataset.DatasetTemplate;

import java.util.Arrays;

/**
 * Created by yaoqijun.
 * Date:2015-10-28
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class DatasetConfig {

    private static final String FS_DEFAULT_NAME = "fs.default.name";

    private static final String FS_DEFAULT_VALUE = "hdfs://localhost:9000";

    private Configuration hadoopConfiguration;

    public DatasetConfig(){
        hadoopConfiguration = new Configuration();
        hadoopConfiguration.set(FS_DEFAULT_NAME,FS_DEFAULT_VALUE);
    }

    public DatasetRepositoryFactory datasetRepositoryFactory(){
        DatasetRepositoryFactory datasetRepositoryFactory = new DatasetRepositoryFactory();
        datasetRepositoryFactory.setConf(hadoopConfiguration);
        datasetRepositoryFactory.setBasePath("/user/spring");
        return datasetRepositoryFactory;
    }

    public DataStoreWriter<FileInfo> dataStoreWriter(){
        return new AvroPojoDatasetStoreWriter<FileInfo>(FileInfo.class, datasetRepositoryFactory(), fileInfoDatasetDefinition());
    }

    public DatasetOperations datasetOperations() {
        DatasetTemplate datasetOperations = new DatasetTemplate();
        datasetOperations.setDatasetDefinitions(Arrays.asList(fileInfoDatasetDefinition()));
        datasetOperations.setDatasetRepositoryFactory(datasetRepositoryFactory());
        return datasetOperations;
    }

    public DatasetDefinition fileInfoDatasetDefinition() {
        DatasetDefinition definition = new DatasetDefinition();
        definition.setFormat(Formats.AVRO.getName());
        definition.setTargetClass(FileInfo.class);
        definition.setAllowNullValues(false);
        return definition;
    }
}
