package fire.hdfsio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by jayantshekhar
 */
public class Delete {

    public static void deleteFile(String str) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete( new Path(str));
    }
}
