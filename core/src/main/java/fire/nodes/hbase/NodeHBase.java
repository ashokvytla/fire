package fire.nodes.hbase;

import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;


/**
 * Created by Ashok Rajan on 11/16/15.
 */
public class NodeHBase extends Node implements Serializable {

    public NodeHBase(int i, String nm) {
        super(i, nm);
    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing HBaseNode : " + id);

        //testMapping(sqlContext);
       // mapColumns(sqlContext);

        writeToHbase(batchPuts(df));

        super.execute(ctx, sqlContext, workflowContext, df);
    }

    private void testMapping(SQLContext sqlContext){
        String mapperConfigFile = "/home/cloudera/fireprojects/fire/data/hbasemapper.json";
        DataFrame people = sqlContext.jsonFile(mapperConfigFile);
        people.registerTempTable("people");

        DataFrame schools = people.select("schools");
        schools.registerTempTable("schools");
        System.out.println("========= schools.printSchema()===========");
        schools.printSchema();
        System.out.println("========= schools.show();============");
        schools.show();
        System.out.println("=====================");

        Row[] rows = schools.collect();
        Row row = rows[0];


        DataFrame school = schools.select("sname","year");
        school.registerTempTable("school");
        System.out.println("========= school.printSchema()===========");

        school.printSchema();
        System.out.println("========= school.printSchema()===========");
        school.show();
        System.out.println("===================");


    }

    private void mapColumns(SQLContext sqlContext) {
        String mapperConfigFile = "/home/cloudera/fireprojects/fire/data/hbasemapper.json";

        DataFrame mapperConfig = sqlContext.jsonFile(mapperConfigFile);
        mapperConfig.registerTempTable("mapperConfig");

        System.out.println("=========mapperConfig.printSchema() ============");
        mapperConfig.printSchema();
        System.out.println("==========================================");

        DataFrame columnfm = sqlContext.sql("SELECT columnfamily.column FROM mapperConfig");
        Iterator<Row> columnsIterator  = columnfm.collectAsList().iterator();
        while(columnsIterator.hasNext()){
            Row row = columnsIterator.next();
            System.out.println(row.get(0));
        }

//        List<Row> mappers = mapperConfig.collectAsList();
//        Iterator<Row> rowIterator = mappers.iterator();
//        while (rowIterator.hasNext()) {
//            Row mapping = rowIterator.next();
//            System.out.println(mapping.toString());
//        }

    }

    private List<Put> batchPuts(DataFrame df) {
        List<Put> putList = new ArrayList<Put>();
        Put p = null;
        Iterator<Row> rows = df.collectAsList().iterator();
        while (rows.hasNext()) {
            Row row = rows.next();
            System.out.println(row.get(0) + "," + row.get(1) + "," + row.get(2) + "," + row.get(3));

            p = new Put(Bytes.toBytes(row.getString(0)));
            p.addColumn(Bytes.toBytes("persondetails"), Bytes.toBytes("fullname"), Bytes.toBytes(row.getString(1)));
            p.addColumn(Bytes.toBytes("persondetails"), Bytes.toBytes("age"), Bytes.toBytes(row.getString(2)));
            p.addColumn(Bytes.toBytes("persondetails"), Bytes.toBytes("city"), Bytes.toBytes(row.getString(3)));
            putList.add(p);
        }
        return putList;
    }



    private void writeToHbase(List<Put> putList){
        Connection conn = null;
        try{
            Configuration configuration = HBaseConfiguration.create();
            configuration.addResource(new Path("/etc/hbase/conf.cloudera.hbase/hbase-site.xml"));
            conn =   ConnectionFactory.createConnection(configuration);
            Table t1 = conn.getTable(TableName.valueOf("person"));
            t1.put(putList);
            t1.close();
            Admin admin = conn.getAdmin();
            conn.close();
        }catch (IOException e){
            System.out.println("Error writing to HBase "+e.getMessage());
        }
    }

}
