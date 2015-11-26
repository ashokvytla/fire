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

package fire.nodes.dataset;

import fire.util.spark.SchemaUtil;
import fire.workflowengine.WorkflowContext;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.ghost4j.document.PDFDocument;
import org.ghost4j.renderer.SimpleRenderer;
import scala.Tuple2;

import java.awt.*;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jayantshekhar
 * Represents a Dataset Node which points to data in a File or Directory with data in csv format.
 */

public class NodeDatasetPDFImage extends NodeDatasetFileOrDirectory implements Serializable {

    // filename column
    public String filenamecol = "file";

    // image column
    public String imagecol = "image";

    public NodeDatasetPDFImage(int i, String nm, String p) {
        super(i, nm, p);
    }

    public NodeDatasetPDFImage(int i, String nm, String p, String cols, String colTypes, String colmlTypes) {
        super(i, nm, p, cols, colTypes);
    }

    public NodeDatasetPDFImage()
    {

    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodeDatasetPDFImage : " + id);

        JavaPairRDD<String, PortableDataStream> files = ctx.binaryFiles(path);

        JavaRDD<Row> imagesrdd =  files.flatMap(new ConvertFunction());

        // create a schema for the column name and Type of STRING
        StructType schema = SchemaUtil.getSchema(filenamecol + " " + imagecol, "string bytes");

        // Apply the schema to the RDD.
        DataFrame imagedf = sqlContext.createDataFrame(imagesrdd, schema);

        workflowContext.outSchema(imagedf);

        super.execute(ctx, sqlContext, workflowContext, imagedf);
    }

    //------------------------------------------------------------------------------------------------------

}

class ConvertFunction implements FlatMapFunction<Tuple2<String, PortableDataStream>, Row> {

    public Iterable<Row> call(Tuple2<String, PortableDataStream> f) throws Exception {

        PDFDocument document = new PDFDocument( );
        document.load(f._2().open());
        f._2().close();

        SimpleRenderer renderer = new SimpleRenderer();
        renderer.setResolution(300);
        List<Image> images = renderer.render(document);

        LinkedList<Row> ll = new LinkedList<Row>();
        for (int i=0; i<images.size(); i++) {
            ll.add(RowFactory.create(f._1(), images.get(i)));
        }

        return ll;
    }
}

