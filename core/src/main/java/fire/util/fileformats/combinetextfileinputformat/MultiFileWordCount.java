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

package fire.util.fileformats.combinetextfileinputformat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MultiFileWordCount is an example to demonstrate the usage of 
 * MultiFileInputFormat. This examples counts the occurrences of
 * words in the text files under the given input directory.
 */
public class MultiFileWordCount extends Configured implements Tool {


    public static class MapClass extends
            Mapper<WordOffset, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(WordOffset key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private void printUsage() {
        System.out.println("Usage : multifilewc <input_dir> <output>" );
    }

    public int run(String[] args) throws Exception {

        if(args.length < 2) {
            printUsage();
            return 2;
        }

        Job job = new Job(getConf());
        job.setJobName("MultiFileWordCount");
        job.setJarByClass(MultiFileWordCount.class);

        //set the InputFormat of the job to our InputFormat
        job.setInputFormatClass(CombineFileTextInputFormat.class);

        // the keys are words (strings)
        job.setOutputKeyClass(Text.class);
        // the values are counts (ints)
        job.setOutputValueClass(IntWritable.class);

        //use the defined mapper
        job.setMapperClass(MapClass.class);
        //use the WordCount Reducer
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new MultiFileWordCount(), args);
        System.exit(ret);
    }

}