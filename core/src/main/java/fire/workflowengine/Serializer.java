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

package fire.workflowengine;

import fire.util.spark.CreateSparkContext;
import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;
import fire.nodes.ml.NodeDatasetSplit;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Created by jayantshekhar
 */
public class Serializer {

    public static String tojson(NodeResult nr) {

        try {
            // http://wiki.fasterxml.com/JacksonPolymorphicDeserialization
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
            mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);


            String str = mapper.writeValueAsString(nr);

            return str;
        } catch(Exception ex) {
            ex.printStackTrace();
            return "";
        }
    }

    //--------------------------------------------------------------------------------------

    public static String tojson(Workflow wf) {

      try {
          // http://wiki.fasterxml.com/JacksonPolymorphicDeserialization
          ObjectMapper mapper = new ObjectMapper();
          mapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
          mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);


          String str = mapper.writeValueAsString(wf);

          return str;
      } catch(Exception ex) {
          ex.printStackTrace();
          return "";
      }
    }

    //--------------------------------------------------------------------------------------

    public static Workflow fromjson(String json) {

      try {
          // http://wiki.fasterxml.com/JacksonPolymorphicDeserialization
          ObjectMapper mapper = new ObjectMapper();
          mapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
          mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

          // http://stackoverflow.com/questions/4486787/jackson-with-json-unrecognized-field-not-marked-as-ignorable
          mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

          Workflow wf = mapper.readValue(json, Workflow.class);

          return wf;
      } catch(Exception ex) {
          ex.printStackTrace();
          return null;
      }
    }

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {
      try {

          NodeResult nodeResult = new NodeResult(5);
          String sss = tojson(nodeResult);
          System.out.println(sss);


          // create spark and sql context
          JavaSparkContext ctx = CreateSparkContext.create(args);

          SQLContext sqlContext = new SQLContext(ctx);

          WorkflowContext workflowContext = new WorkflowContext();

          // create workflow
          Workflow wf = createWorkflow(ctx, sqlContext);

          // serialize to json
          String json = tojson(wf);
          System.out.println("\n" + json + "\n");

          // create workflow from json
          Workflow newwf = fromjson(json);

          // serialize again from
          json = tojson(newwf);
          System.out.println("\n" + json + "\n");

          // execute the workflow
          newwf.execute(ctx, sqlContext, workflowContext);

          // stop the context
          ctx.stop();

      }
      catch(Exception ex) {
          ex.printStackTrace();
      }
    }

    //--------------------------------------------------------------------------------------

    private static Workflow createWorkflow(JavaSparkContext ctx, SQLContext sqlContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/cars.csv",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNodeDataset(csv1);

        // test schema
        NodeSchema schema = wf.getSchema(1);
        if (schema != null)
            System.out.println(schema.toString());

        // split node
        Node split = new NodeDatasetSplit(7, "split node");
        csv1.addNode(split);


        return wf;
    }

}
