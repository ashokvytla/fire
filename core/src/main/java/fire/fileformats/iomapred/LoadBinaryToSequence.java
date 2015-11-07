/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fire.fileformats.iomapred;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class LoadBinaryToSequence extends Configured implements Tool {


    public static class MapClass extends MapReduceBase implements
            Mapper<Text, BytesWritable, Text, BytesWritable> {

        private Text word = new Text();

        public void map(Text key, BytesWritable value, OutputCollector<Text, BytesWritable> output, Reporter reporter)
                throws IOException {

            output.collect(key, value);
        }
    }

    private void printUsage() {
        System.out.println("Usage : LoadBinaryToSequence <input_dir> <output>" );
    }

    public int run(String[] args) throws Exception {

        if(args.length < 2) {
            printUsage();
            return 2;
        }

        JobConf conf = new JobConf(LoadBinaryToSequence.class);
        conf.setJobName("loadbinarytosequence");

        //set the InputFormat of the job to our InputFormat
        conf.setInputFormat(CombineFileBinaryInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        // the keys are words (strings)
        conf.setOutputKeyClass(Text.class);
        // the values are images
        conf.setOutputValueClass(BytesWritable.class);

        //use the defined mapper
        conf.setMapperClass(MapClass.class);

        FileInputFormat.addInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new LoadBinaryToSequence(), args);
        System.exit(ret);
    }

}