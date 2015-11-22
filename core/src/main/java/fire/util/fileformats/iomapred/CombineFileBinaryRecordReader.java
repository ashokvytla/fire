package fire.util.fileformats.iomapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * RecordReader is responsible from extracting records from a chunk
 * of the CombineFileSplit.
 */
public class CombineFileBinaryRecordReader
        implements RecordReader<Text, BytesWritable> {

    private FileSystem fs;
    private Path path;
    private Text key;
    private BytesWritable value;

    private FSDataInputStream fileIn;
    private LineReader reader;

    private int numRecordsRead = 0;

    public CombineFileBinaryRecordReader(Configuration conf, CombineFileSplit split,
                                         Reporter reporter, Integer index) throws IOException {
        this.path = split.getPath(index);
        fs = this.path.getFileSystem(conf);
    }

    public CombineFileBinaryRecordReader(CombineFileSplit split, Configuration conf,
                                         Reporter reporter, Integer index) throws IOException {
        this.path = split.getPath(index);
        fs = this.path.getFileSystem(conf);
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
    }

    public void close() throws IOException { }

    public float getProgress() throws IOException {
        if (numRecordsRead == 0)
            return 0.0f;

        return 1.0f;
    }

    @Override
    public Text createKey() {
        // TODO Auto-generated method stub
        return new Text(path.getName());
    }

    @Override
    public BytesWritable createValue() {
        // TODO Auto-generated method stub
        return new BytesWritable();
    }

    public boolean next(Text key, BytesWritable value) throws IOException {
        if (numRecordsRead > 0)
            return false;

        if (key == null) {
            key = new Text(path.getName());
        }
        if (value == null) {
            value = new BytesWritable();
        }

        FSDataInputStream in = null;
        try {
            in = fs.open(path);
            byte buffer[] = new byte[in.available()];
            in.read(buffer);

            BytesWritable data = new BytesWritable(buffer);
            value.set(data);
        } finally {
            IOUtils.closeStream(in);
        }

        numRecordsRead++;

        return true;
    }

    public Text getCurrentKey()
            throws IOException, InterruptedException {
        return key;
    }

    @Override
    public long getPos() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
}