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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.bytedeco.javacpp.lept;
import org.bytedeco.javacpp.tesseract;
import org.ghost4j.document.PDFDocument;
import org.ghost4j.renderer.SimpleRenderer;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static org.bytedeco.javacpp.lept.pixDestroy;
import static org.bytedeco.javacpp.lept.pixReadMem;

/**
 * Created by jayantshekhar
 * Represents a Dataset Node which points to data in a File or Directory with data in csv format.
 */

public class NodeDatasetPDFImageOCR extends NodeDatasetFileOrDirectory implements Serializable {

    // filename column
    public String filenamecol = "file";

    // image column
    public String imagecol = "image";

    public NodeDatasetPDFImageOCR(int i, String nm, String p) {
        super(i, nm, p);
    }

    public NodeDatasetPDFImageOCR(int i, String nm, String p, String cols, String colTypes, String colmlTypes) {
        super(i, nm, p, cols, colTypes);
    }

    public NodeDatasetPDFImageOCR()
    {

    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodeDatasetPDFImageOCR : " + id);

        JavaPairRDD<String, PortableDataStream> files = ctx.binaryFiles(path);

        JavaRDD<Row> imagesrdd =  files.flatMap(new ExtractOCRFunction());

        imagesrdd.count();

        // create a schema for the column name and Type of STRING
        StructType schema = SchemaUtil.getSchema(filenamecol + " " + imagecol, "string string");

        // Apply the schema to the RDD.
        DataFrame imagedf = sqlContext.createDataFrame(imagesrdd, schema);

        workflowContext.outSchema(imagedf);

        super.execute(ctx, sqlContext, workflowContext, imagedf);
    }

    //------------------------------------------------------------------------------------------------------

}

class ExtractOCRFunction implements FlatMapFunction<Tuple2<String, PortableDataStream>, Row> {

    public Iterable<Row> call(Tuple2<String, PortableDataStream> f) throws Exception {

        PDFDocument document = new PDFDocument( );
        document.load(f._2().open());
        f._2().close();

        SimpleRenderer renderer = new SimpleRenderer();
        renderer.setResolution(300);
        List<Image> images = renderer.render(document);

        /**  Iterate through the image list and extract OCR
         * using Tesseract API.
         */

        StringBuilder r = new StringBuilder();
        for (int i=0; i<images.size(); i++) {
            ByteArrayOutputStream imageByteStream = new ByteArrayOutputStream();
            ImageIO.write((RenderedImage)images.get(i), "png", imageByteStream);

            lept.PIX pix = pixReadMem(
                    ByteBuffer.wrap(imageByteStream.toByteArray()).array( ),
                    ByteBuffer.wrap( imageByteStream.toByteArray( ) ).capacity( )
            );

            tesseract.TessBaseAPI api = new tesseract.TessBaseAPI();
            /** We assume the documents are in English here, hence \”eng\” */
            api.Init( null, "eng" );
            api.SetImage(pix);
            r.append(api.GetUTF8Text().getString());
            imageByteStream.close();
            pixDestroy(pix);
            api.End();

        }

        Row row = RowFactory.create(f._1(), r.toString());

        LinkedList temp = new LinkedList();
        temp.add(row);

        return temp;
    }

}

