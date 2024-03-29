package rdfstream2flink.runner.functions.filter;

//import com.hp.hpl.jena.graph.Node;
//import com.hp.hpl.jena.sparql.expr.E_NotEquals;
//import com.hp.hpl.jena.sparql.expr.Expr;


import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;

import java.util.HashMap;

public class NotEquals {

    Expr arg1 = null;
    Expr arg2 = null;

    public NotEquals(E_NotEquals expresion){
        this.arg1 = expresion.getArg1();
        this.arg2 = expresion.getArg2();
    }

    public boolean eval(HashMap<String, Node> solutionMapping){
        Boolean flag = false;
        Node value_left = null;
        Node value_right = null;

        if(arg1.isConstant() && arg2.isVariable()) {
            value_left = arg1.getConstant().getNode();
            value_right = solutionMapping.get(arg2.toString());
        } else if(arg1.isVariable() && arg2.isConstant()) {
            value_left = solutionMapping.get(arg1.toString());
            value_right = arg2.getConstant().getNode();
        } else if(arg1.isVariable() && arg2.isVariable()) {
            value_left = solutionMapping.get(arg1.toString());
            value_right = solutionMapping.get(arg1.toString());
        }

        if(value_left.isLiteral()) {
            if (!(value_left.getLiteralValue().toString().equals(value_right.getLiteralValue().toString()))) {
                //System.out.println("--- they are NotEquals ---");
                flag = true;
            }
        } else if(value_left.isURI()) {
            if (!(value_left.toString().equals(value_right.toString()))) {
                //System.out.println("--- they are NotEquals ---");
                flag = true;
            }
        }

        return flag;
    }
}
