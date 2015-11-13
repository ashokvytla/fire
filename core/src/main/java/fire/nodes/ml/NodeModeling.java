package fire.nodes.ml;

import fire.workflowengine.Node;
import org.apache.spark.ml.Model;

/**
 * Created by jayantshekhar on 11/12/15.
 */

// represents a node that creates predictive models
public class NodeModeling extends Node {

    public String labelColumn = "label";
    public String predictorColumns = "f1 f2";

    public NodeModeling()
    {}

    public NodeModeling(int i, String nm) {
        super(i, nm);
    }

    public NodeModeling(int i, String nm, String lcol, String pcols) {
        super(i, nm);

        labelColumn = lcol;
        predictorColumns = pcols;
    }

    // pass the model to the scoring nodes
    public void passModel(Model model) {

        // pass the computed model to the next node if it is a scoring node
        Node nextNode = this.getNode(0);
        if (nextNode != null)
        {
            if (nextNode instanceof NodeModelScore)
            {
                NodeModelScore score = (NodeModelScore)nextNode;
                score.model = model;
                score.labelColumn = this.labelColumn;
                score.predictorColumns = this.predictorColumns;
            }
        }
    }
}
