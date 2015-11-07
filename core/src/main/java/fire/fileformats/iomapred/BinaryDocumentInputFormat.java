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

package fire.fileformats.iomapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reads complete documents in Binary format.
 */
public class BinaryDocumentInputFormat extends FileInputFormat<Text, BytesWritable> {

    public BinaryDocumentInputFormat() {
        super();
    }

    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

        return new BinaryDocumentWithNameRecordReader((FileSplit) split, job);
    }

    /**
     * BinaryDocumentWithNameRecordReader class to read through a given binary
     * document Outputs the filename along with the complete document
     */
    public class BinaryDocumentWithNameRecordReader implements RecordReader<Text, BytesWritable> {

        private FileSplit fileSplit;
        private Configuration conf;
        private boolean processed = false;

        public BinaryDocumentWithNameRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
            this.fileSplit = fileSplit;
            this.conf = conf;
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public BytesWritable createValue() {
            // Initializing this with a byte array of the correct size prevents
            // the BytesWritable from triggering a resize, which will exceed
            // Integer.MAX_VALUE for input files > 1.4G or so.
            return new BytesWritable(new byte[(int) this.fileSplit.getLength()]);
        }

        @Override
        public long getPos() throws IOException {
            return this.processed ? this.fileSplit.getLength() : 0;
        }

        @Override
        public float getProgress() throws IOException {
            return this.processed ? 1.0f : 0.0f;
        }

        @Override
        public boolean next(Text key, BytesWritable value) throws IOException {
            if (!this.processed) {
                byte[] contents = new byte[(int) this.fileSplit.getLength()];
                Path file = this.fileSplit.getPath();
                FileSystem fs = file.getFileSystem(this.conf);
                FSDataInputStream in = null;

                try {
                    in = fs.open(file);
                    in.readFully(0, contents, 0, contents.length);

                    key.set(file.getName());
                    value.set(contents, 0, contents.length);
                } finally {
                    in.close();
                }

                this.processed = true;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void close() throws IOException {
        }
    }
}