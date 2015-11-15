package fire.nodes.etl;

import fire.nodes.ml.NodeModelScore;
import fire.workflowengine.Node;
import org.apache.spark.ml.Model;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;

import java.util.Iterator;

/**
 * Created by jayantshekhar on 11/14/15.
 */
public abstract class NodeETL extends Node {

    public NodeETL()
    {}

    public NodeETL(int i, String nm) {
        super(i, nm);
    }

}
