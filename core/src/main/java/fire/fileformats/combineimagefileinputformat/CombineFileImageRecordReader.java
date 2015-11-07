/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fire.fileformats.combineimagefileinputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * RecordReader is responsible from extracting records from a chunk
 * of the CombineFileSplit.
 */
public class CombineFileImageRecordReader
        extends RecordReader<Text, BytesWritable> {

    private FileSystem fs;
    private Path path;
    private Text key;
    private BytesWritable value;

    private FSDataInputStream fileIn;
    private LineReader reader;

    private int numRecordsRead = 0;

    public CombineFileImageRecordReader(CombineFileSplit split,
                                       TaskAttemptContext context, Integer index) throws IOException {
        this.path = split.getPath(index);
        fs = this.path.getFileSystem(context.getConfiguration());
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

    public boolean nextKeyValue() throws IOException {
        if (numRecordsRead > 0)
            return false;

        if (key == null) {
            key = new Text(path.getName());
        }
        if (value == null) {
            value = new BytesWritable();
        }

        //String uri = key.toString();
        //Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        try {

            /***
            in = fs.open(path);
            java.io.ByteArrayOutputStream bout = new ByteArrayOutputStream();
            byte buffer[] = new byte[1024 * 1024];

            while( in.read(buffer, 0, buffer.length) >= 0 ) {
                bout.write(buffer);
            }
             ***/

            in = fs.open(path);
            byte buffer[] = new byte[in.available()];
            in.read(buffer);

            value = new BytesWritable(buffer);
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

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
}

