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

package fire.fileformats.combinetextfileinputformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This record keeps &lt;filename,offset&gt; pairs.
 */
public class WordOffset implements WritableComparable {

    public long offset;
    public String fileName;

    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    public int compareTo(Object o) {
        WordOffset that = (WordOffset)o;

        int f = this.fileName.compareTo(that.fileName);
        if(f == 0) {
            return (int)Math.signum((double)(this.offset - that.offset));
        }
        return f;
    }
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof WordOffset)
            return this.compareTo(obj) == 0;
        return false;
    }
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; //an arbitrary constant
    }
}