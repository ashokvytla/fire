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

package fire.fileformats.tika;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;


public class TikaRecordReader extends RecordReader<Text, Text>
{

	private CombineFileSplit split;
	private FileSystem fs;
	private Text key, value;
	private Path[] paths;
	private FSDataInputStream currentStream;
	private InputStream is;
	public Tika tika;

	// count and done are used for progress
	private int count = 0;
	private boolean done = false;

	public TikaRecordReader(CombineFileSplit split, TaskAttemptContext context)
			throws IOException
	{
		this.paths = split.getPaths();
		this.fs = FileSystem.get(context.getConfiguration());
		this.split = split;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		if (count >= split.getNumPaths())
		{
			done = true;
			return false; // we have no more data to parse
		}

		Path path;
		path = null;
		key = new Text();
		value = new Text();

		try
		{
			path = this.paths[count];
		} catch (Exception e)
		{
			return false;
		}

		currentStream = null;
		currentStream = fs.open(path);
		is=currentStream;
		
		tika = new Tika();
		tika.setMaxStringLength(-1);
		String content;
		try {
			key.set(path.getName());
			content = tika.parseToString(is);
			value.set(content);
			
		} catch (TikaException e) {
			e.printStackTrace();
		} 

		currentStream.close();
		count++;

		return true; // we have more data to parse
	}

	@Override
	public void close() throws IOException
	{
		done = true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException
	{
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException
	{
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		return done ? 1.0f : (float) (count / paths.length);
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException
	{
	}
}
