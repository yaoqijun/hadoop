package org.yqj.hadoop.spring;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.Formats;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.dataset.AvroPojoDatasetStoreWriter;
import org.springframework.data.hadoop.store.dataset.DatasetDefinition;
import org.springframework.data.hadoop.store.dataset.DatasetRepositoryFactory;
import org.springframework.data.hadoop.store.dataset.DatasetTemplate;

import java.util.Arrays;

/**
 * Created by yaoqijun.
 * Date:2015-10-28
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
@Data
public class DatasetConfig {

    private static final String FS_DEFAULT_NAME = "fs.default.name";

    private static final String FS_DEFAULT_VALUE = "hdfs://localhost:9000";

    private Configuration hadoopConfiguration;

    private DatasetRepositoryFactory datasetRepositoryFactory;

    private DatasetDefinition datasetDefinition;

    private DataStoreWriter<FileInfo> dataStoreWriter;

    private DatasetTemplate datasetTemplate;

    public DatasetConfig(){
        hadoopConfiguration = new Configuration();
        hadoopConfiguration.set(FS_DEFAULT_NAME, FS_DEFAULT_VALUE);

        datasetRepositoryFactory = new DatasetRepositoryFactory();
        datasetRepositoryFactory.setConf(hadoopConfiguration);
        datasetRepositoryFactory.setBasePath("/user/yaoqijun/test");

        datasetDefinition = new DatasetDefinition();
        datasetDefinition.setFormat(Formats.AVRO.getName());
        datasetDefinition.setTargetClass(FileInfo.class);
        datasetDefinition.setAllowNullValues(false);

        dataStoreWriter = new AvroPojoDatasetStoreWriter(FileInfo.class, datasetRepositoryFactory, datasetDefinition);

        datasetTemplate = new DatasetTemplate();
        datasetTemplate.setDatasetDefinitions(Arrays.asList(datasetDefinition));
        datasetTemplate.setDatasetRepositoryFactory(datasetRepositoryFactory);
    }
}
