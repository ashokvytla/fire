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

package fire.noderesult;

import fire.workflowengine.Result;

/**
 * Created by jayantshekhar
 */
public class NodeResultGraph extends Result {

    public String title; // graph title

    public int[] x; // x coordinates
    public String xlabel; // xlabel

    public double[] y; // y values
    public String ylabel; // ylabel

    public NodeResultGraph(int nid, String ttitle, int tx[], String txlabel, double[] ty, String tylabel) {

        super(nid);

        title = ttitle;

        x = tx;
        xlabel = txlabel;

        y = ty;
        ylabel = tylabel;

    }

}
