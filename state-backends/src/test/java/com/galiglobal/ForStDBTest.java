package com.galiglobal;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyHandle;
import org.forstdb.DBOptions;
import org.forstdb.FlinkEnv;
import org.forstdb.RocksDB;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ForStDBTest {

    @ClassRule
    public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    public void testFileSystemInit() throws Exception {

//            Path localBasePath = new Path(TMP_FOLDER.newFolder().getPath());
//        Path remoteBasePath = new Path(TMP_FOLDER.newFolder().getPath());
//        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
//        ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);
//        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
//        DBOptions dbOptions2 =
//            new DBOptions().setCreateIfMissing(true).setAvoidFlushDuringShutdown(true);
//        ForStFlinkFileSystem fileSystem = ForStFlinkFileSystem.get(remoteBasePath.toUri(), localBasePath, null);
//
//        dbOptions2.setEnv(
//            new FlinkEnv(
//                remoteBasePath.toString(), new StringifiedForStFileSystem(fileSystem)));
//                //remoteBasePath.toString(), new ForStFileSystem(fileSystem)));
//        RocksDB db =
//            RocksDB.open(
//                dbOptions2,
//                remoteBasePath.getPath(),
//                columnFamilyDescriptors,
//                columnFamilyHandles);
//        db.put("key".getBytes(), "value".getBytes());
//        db.getSnapshot();
//        db.close();
    }

}
