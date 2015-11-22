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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jayantshekhar
 */
public class FireSchema {

    // column names, column names do not have space in them
    public String[] columnNames;

    // column types. we are using avro types
    // Schema.Type.STRING : string | Schema.Type.DOUBLE : double | Schema.Type.INT : int
    public org.apache.avro.Schema.Type[] columnTypes;

    // column ML types : numeric | categorical | string
    public final static int TYPE_NUMERIC = 0;
    public final static int TYPE_CATEGORICAL = 1;
    public final static int TYPE_TEXT = 2;
    public int[] columnMLTypes;

    //--------------------------------------------------------------------------------------

    // constructor
    public FireSchema() {

    }

    public int getColIdx(String col) {
        for (int i=0; i<columnNames.length; i++) {
            if (col.equals(columnNames[i]))
                return i;
        }

        return -1;
    }
    //--------------------------------------------------------------------------------------

    // constructor
    public FireSchema(String[] cnames, org.apache.avro.Schema.Type[] ctypes) {
        columnNames = cnames;
        columnTypes = ctypes;

        // set all column ml types to numeric is it is not provided
        columnMLTypes = new int[columnNames.length];
        for (int i=0; i<columnMLTypes.length; i++)
            columnMLTypes[i] = TYPE_NUMERIC;
    }

    //--------------------------------------------------------------------------------------

    // constructor
    public FireSchema(String[] cnames, org.apache.avro.Schema.Type[] ctypes, int[] cmltypes) {
        columnNames = cnames;
        columnTypes = ctypes;
        columnMLTypes = cmltypes;
    }

    //--------------------------------------------------------------------------------------

    // the 3 input parameters are provided as space separated strings
    public FireSchema(String colnames, String coltypes, String colmltypes) {

        // columnNames array
        columnNames = colnames.split(" ");

        // col types array
        final String coltypesarr[] = coltypes.split(" ");

        // col ml types array
        final String colmltypesarr[] = colmltypes.split(" ");

        // col types
        int idx = 0;
        columnTypes = new org.apache.avro.Schema.Type[columnNames.length];

        for (String coltype : coltypesarr) {
            org.apache.avro.Schema.Type type = null;
            switch(coltype)
            {
                case "int" : type = org.apache.avro.Schema.Type.INT; break;
                case "double" : type = org.apache.avro.Schema.Type.DOUBLE; break;
                case "string" : type = org.apache.avro.Schema.Type.STRING; break;
            }

            columnTypes[idx] = type;

            idx++;
        }

        // col ML types
        idx = 0;
        columnMLTypes = new int[colmltypesarr.length];
        for (String colmlType : colmltypesarr) {
            switch(colmlType)
            {
                case "numeric" : columnMLTypes[idx] = TYPE_NUMERIC; break;
                case "categorical" : columnMLTypes[idx] = TYPE_CATEGORICAL; break;
                case "string" : columnMLTypes[idx] = TYPE_TEXT; break;
            }

            idx++;
        }
    }

    //--------------------------------------------------------------------------------------

    @Override
    public String toString() {
        String string = "";
        for (String str : columnNames)
            string += str + " ";

        for (org.apache.avro.Schema.Type typ : columnTypes)
            string += typ + " ";

        for (int mltyp : columnMLTypes)
            string += mltyp + " ";

        return string;
    }

    //--------------------------------------------------------------------------------------

    // used for DataFrame manipulation. convert from Avro types to Spark SQL StructType
    public StructType getSparkSQLStructType() {

        List<StructField> fields = new ArrayList<StructField>();

        int idx = 0;
        for (String fieldName: columnNames) {
            DataType dataType = null;

            if (columnTypes[idx] == org.apache.avro.Schema.Type.INT)
            {
                dataType = DataTypes.IntegerType;
            } else if (columnTypes[idx] == org.apache.avro.Schema.Type.DOUBLE)
            {
                dataType = DataTypes.DoubleType;
            } else if (columnTypes[idx] == org.apache.avro.Schema.Type.STRING)
            {
                dataType = DataTypes.StringType;
            }

            fields.add(DataTypes.createStructField(fieldName, dataType, true)); // name, datatype, nullable

            idx++;
        }

        StructType structType = DataTypes.createStructType(fields);

        return structType;
    }

    //--------------------------------------------------------------------------------------

    // create a new schema by joining a given schema to this schema on a given column
    public FireSchema join(FireSchema sch, String joincol) {
        ArrayList<String> cnames = new ArrayList();
        ArrayList<org.apache.avro.Schema.Type> ctypes = new ArrayList<>();

        int idx = 0;
        for (int i=0; i<columnNames.length; i++) {
            cnames.add(columnNames[i]);
            ctypes.add(columnTypes[i]);
        }

        for (int i=0; i<sch.columnNames.length; i++) {

            if (joincol.equals(sch.columnNames[i]))
                continue;

            cnames.add(sch.columnNames[i]);
            ctypes.add(sch.columnTypes[i]);
        }

        return new FireSchema(cnames.toArray(new String[cnames.size()]), ctypes.toArray(new org.apache.avro.Schema.Type[ctypes.size()]));
    }

    //--------------------------------------------------------------------------------------

}
