package fire.nodes.ml;

import fire.workflowengine.Node;
import org.apache.spark.ml.Model;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;

import java.util.Iterator;

/**
 * Created by jayantshekhar on 11/12/15.
 */

// represents a node that creates predictive models
public abstract class NodeModeling extends Node {

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
        Iterator<Node> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            Node nextNode = iterator.next();

            if (nextNode instanceof NodeModelScore)
            {
                NodeModelScore score = (NodeModelScore)nextNode;
                score.model = model;
                score.labelColumn = this.labelColumn;
                score.predictorColumns = this.predictorColumns;
            }
        }

    }

    // pass GLM to the scoring nodes
    public void passModel(GeneralizedLinearModel glm) {

        // pass the computed model to the next node if it is a scoring node
        Iterator<Node> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            Node nextNode = iterator.next();

            if (nextNode instanceof NodeModelScore)
            {
                NodeModelScore score = (NodeModelScore)nextNode;
                score.glm = glm;
                score.labelColumn = this.labelColumn;
                score.predictorColumns = this.predictorColumns;
            }
        }
        
    }
}
