package fire.sparkutil;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class CreateSparkContext {

    public static JavaSparkContext create(String[] args) {

        // set local to true when running in intellij or eclipse
        // set it to false when running on the cluster
        boolean isLocal = true;

        if (args.length > 0 && args[0].equals("cluster"))
            isLocal = false;

        //System.setProperty("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        //System.setProperty("spark.kryo.registrator","Registrator");

        System.setProperty("spark.akka.timeout","900");
        System.setProperty("spark.worker.timeout","900");
        System.setProperty("spark.storage.blockManagerSlaveTimeoutMs","3200000");

        // create spark context
        SparkConf sparkConf = new SparkConf().setAppName("RunLogisticRegression");
        if (isLocal) {
            sparkConf.setMaster("local");
            sparkConf.set("spark.broadcast.compress", "false");
            sparkConf.set("spark.shuffle.compress", "false");
        }

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        return ctx;
    }

}
