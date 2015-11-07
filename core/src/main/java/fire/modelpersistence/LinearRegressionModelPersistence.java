package fire.modelpersistence;

import org.apache.spark.mllib.regression.LinearRegressionModel;

/**
 * Created by jayantshekhar
 */
public class LinearRegressionModelPersistence {

    public static String toJson(LinearRegressionModel model) {

        String str = model.toString();

        return str;
    }
}
