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

package fire.util.fileformats.pdf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class PdfRecordReader extends RecordReader<Text, Text> {

	private FileSplit fileSplit;
	private Configuration conf;
	private Text key = null;
	private Text value = null;
	private boolean processed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();

	}

//	InputSplit split = context.getInputSplit();
//	Path path = ((FileSplit) split).getPath();
//	filenameKey = new Text(path.toString());
	
	//Here it reads a pdf files, extracts the contents of it and creates a Text object of the content and returns
	//key=filename
	//value= text content of the pdf file
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if(!processed){
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			PDDocument pdf = null;
			String parsedText = null;
			PDFTextStripper stripper;
			try{
				in = fs.open(file);
				pdf = PDDocument.load(in);
				stripper = new PDFTextStripper();
				parsedText = stripper.getText(pdf);
				this.key = new Text(file.getName());
				this.value= new Text(parsedText);
			}finally{
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {

		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {

	}

}