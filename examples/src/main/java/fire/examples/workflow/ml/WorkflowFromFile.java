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

package fire.examples.workflow.ml;

import fire.workflowengine.WorkflowContext;
import fire.workflowengine.Serializer;
import fire.sparkutil.CreateSparkContext;
import fire.workflowengine.Workflow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by jayantshekhar
 */
public class WorkflowFromFile {

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        fromfile(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // kmeans workflow
    private static void fromfile(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        String fn = "data/workflows/kmeans.wf";

        String json = readFile(fn);

        Workflow wf = Serializer.fromjson(json);


        // execute the workflow
        wf.execute(ctx, sqlContext, workflowContext);



    }

    public static String readFile(String fileName) {

        // This will reference one line at a time
        String line = null;

        String str = "";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                System.out.println(line);

                str += line;
            }

            // Always close files.
            bufferedReader.close();

            return str;
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }

        return null;
    }
}
