package fire.fileformats.iomapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;

/**
 * To use {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat}, one should extend it, to return a
 * (custom) {@link org.apache.hadoop.mapreduce.RecordReader}. CombineFileInputFormat uses
 * {@link org.apache.hadoop.mapreduce.lib.input.CombineFileSplit}s.
 */
public class CombineFileBinaryInputFormat
        extends CombineFileInputFormat<Text, BytesWritable> {

    public RecordReader<Text,BytesWritable> getRecordReader(InputSplit split,
                                                            JobConf conf, Reporter reporter) throws IOException {


        return new CombineFileRecordReader<Text, BytesWritable>(
                conf, (CombineFileSplit)split, reporter, (Class)CombineFileBinaryRecordReader.class);
    }

    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
