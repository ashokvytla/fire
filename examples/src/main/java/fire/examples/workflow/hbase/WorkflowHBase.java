package fire.examples.workflow.hbase;

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

import fire.util.fileformats.csv.ReadCSV;
import fire.util.spark.CreateSparkContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ashok Rajan
 */
public class WorkflowHBase {

    JavaSparkContext javaSparkContext = null;
    SQLContext sqlContext = null;

    public WorkflowHBase(JavaSparkContext ctx){
        this.javaSparkContext = ctx;
        sqlContext = new SQLContext(ctx);
    }


    private List<Put> readCSV(String filePath){
        JavaPairRDD<String, String> csvData = javaSparkContext.wholeTextFiles(filePath);
        JavaRDD<String[]> rdd = csvData.flatMap(new ReadCSV.ParseLine());

        List<String[]> result = rdd.collect();
        List<Put> putList = new ArrayList<Put>();
        Put p = null;

        for (int i = 0; i < result.size(); i++) {
            String[] arr = result.get(i);
            for (int j = 0; j<arr.length; j++) {
              //  System.out.print(arr[j] + ", ");

                p = new Put(Bytes.toBytes(arr[0]));
                p.addColumn(Bytes.toBytes("persondetails"), Bytes.toBytes("fullname"),Bytes.toBytes(arr[1]));
                p.addColumn(Bytes.toBytes("persondetails"), Bytes.toBytes("age"),Bytes.toBytes(arr[2]));
                p.addColumn(Bytes.toBytes("persondetails"), Bytes.toBytes("city"),Bytes.toBytes(arr[3]));
                putList.add(p);
            }
            //System.out.println("");
        }

        return putList;
    }

    private void writeToHbase(String filePath){
        Connection conn = null;
        try{
            Configuration configuration = HBaseConfiguration.create();
            configuration.addResource(new Path("/etc/hbase/conf.cloudera.hbase/hbase-site.xml"));
            conn =   ConnectionFactory.createConnection(configuration);

            Table t1 = conn.getTable(TableName.valueOf("person"));
            List<Put> putList = null;

            putList = readCSV(filePath);
          //  p = new Put(Bytes.toBytes("rowKey"));
          //  p.addColumn(Bytes.toBytes("columnFamilyName"), Bytes.toBytes("columnName"),Bytes.toBytes("Some Value"));

            t1.put(putList);
            t1.close();
            Admin admin = conn.getAdmin();
            conn.close();
        }catch (IOException e){
            System.out.println("Error HBase "+e.getMessage());
        }
    }

    private void readHBaseMapperConfig(String mapperConfigFile){
      DataFrame df = sqlContext.jsonFile(mapperConfigFile);
      df.printSchema();
    }

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {
        String filePath = "/home/cloudera/fireprojects/fire/data/person.csv";
        String mapperConfigFile = "/home/cloudera/fireprojects/fire/data/hbasemapper.json";
        JavaSparkContext ctx = CreateSparkContext.create(args);

        WorkflowHBase workflowHBase = new WorkflowHBase(ctx);
        //workflowHBase.readCSV(filePath);
        workflowHBase.writeToHbase(filePath);
        // stop the context
        ctx.stop();
    }

}
