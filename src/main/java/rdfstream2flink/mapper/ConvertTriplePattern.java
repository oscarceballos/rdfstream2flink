package rdfstream2flink.mapper;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;


import org.deri.cqels.lang.cqels.OpStream;
import rdfstream2flink.mapper.SolutionMapping;

import java.util.ArrayList;
import java.util.Arrays;

public class ConvertTriplePattern {

    public ConvertTriplePattern(){}

    public static String convertObject(Node node){
        String obj = "";
        if(node.isLiteral()) {
            if (node.getLiteralDatatype().getJavaClass().equals(String.class)){
                obj = "\"" + node.getLiteralValue().toString() + "\"";
            } else {
                obj = node.toString();
            }
        } else if (node.isURI()) {
            obj = "\"" + node.getURI().toString() + "\"";
        }
        return obj;
    }

    public static String convert(Triple triple, Integer indice) {
        return convert(null, triple, indice, null);
    }

    public static String convert(OpStream op, Triple triple, Integer indice, ConvertLQP2FlinkProgram convertLQP2FP) {
        if(op != null) triple = op.getBasicPattern().get(0);

        String filter_map = "";

        String subject_filter = null;
        String predicate_filter = null;
        String object_filter = null;

        String subject_map = null;
        String predicate_map = null;
        String object_map = null;

        ArrayList<String> variables = null;

        if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject_map = "\""+triple.getSubject().toString()+"\"";
            predicate_map = "\""+triple.getPredicate().toString()+"\"";
            object_map = "\""+triple.getObject().toString()+"\"";

            variables = new ArrayList(Arrays.asList(new String[]{subject_map, predicate_map, object_map}));
        } else if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            object_filter = convertObject(triple.getObject());

            subject_map = "\""+triple.getSubject().toString()+"\"";
            predicate_map = "\""+triple.getPredicate().toString()+"\"";

            variables = new ArrayList(Arrays.asList(new String[]{subject_map, predicate_map}));
        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";
            object_filter = convertObject(triple.getObject());

            subject_map = "\""+triple.getSubject().toString()+"\"";

            variables = new ArrayList(Arrays.asList(new String[]{subject_map}));
        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";

            subject_map = "\""+triple.getSubject().toString()+"\"";
            object_map = "\""+triple.getObject().toString()+"\"";

            variables = new ArrayList(Arrays.asList(new String[]{subject_map, object_map}));
        } else if(!triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject_filter = "\""+triple.getSubject().toString()+"\"";

            predicate_map = "\""+triple.getPredicate().toString()+"\"";
            object_map = "\""+triple.getObject().toString()+"\"";

            variables = new ArrayList(Arrays.asList(new String[]{predicate_map, object_map}));
        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject_filter = "\""+triple.getSubject().toString()+"\"";
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";

            object_map = "\""+triple.getObject().toString()+"\"";

            variables = new ArrayList(Arrays.asList(new String[]{object_map}));
        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            subject_filter = "\""+triple.getSubject().toString()+"\"";
            predicate_filter = "\""+triple.getPredicate().toString()+"\"";
            object_filter = convertObject(triple.getObject());
        }

        if(op == null) {
            SolutionMapping.insertSolutionMapping(indice, variables);
            filter_map += "\t\t\t.filter(new Triple2Triple("+subject_filter+", "+predicate_filter+", "+object_filter+"))\n";
            filter_map += "\t\t\t.map(new Triple2SolutionMapping("+subject_map+", "+predicate_map+", "+object_map+"));\n\n";
        }
        else filter_map = convertLQP2FP.generateOpStream(op, Boolean.FALSE, variables);

        return filter_map;
    }
}
