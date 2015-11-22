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

import org.apache.spark.ml.Model;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.RegressionModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * Created by jayantshekhar
 *
 * Workflow Context. We can also inherit from the class to overide its methods and use the inherited class for
 * passing into workflow execution flow.
 *
 */
public class WorkflowContext {

    // array for storing the results of the workflow execution.
    ArrayList<Result> arr = new ArrayList<Result>();

    //-------------------------------------------------------------------------

    public void out(String str) {
        System.out.println(str);
    }

    //-------------------------------------------------------------------------

    public void outSchema(DataFrame df) {

        StructType structType = df.schema();
        String schemaStr = structType.simpleString();

        out(schemaStr);
    }

    //-------------------------------------------------------------------------

    public void outResult(Result result) {

        arr.add(result);

        String string = Serializer.tojson(result);
        out(string);

    }

    //-------------------------------------------------------------------------

    public void out(Exception ex) {

        String string = ex.toString();

        out(string);
    }

    //-------------------------------------------------------------------------


    public void out(Model model) {

        String string = model.toString();

        out(string);
    }

    public void out(RegressionModel model) {
        String string = model.toString();

        out(string);
    }

    public void out(Matrix matrix) {
        String string = matrix.toString();

        out(string);
    }
}
