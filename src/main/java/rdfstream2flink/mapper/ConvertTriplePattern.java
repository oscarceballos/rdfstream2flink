package rdfstream2flink.mapper;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;


import org.deri.cqels.lang.cqels.OpStream;
import rdfstream2flink.mapper.SolutionMapping;

import java.util.ArrayList;
import java.util.Arrays;

public class ConvertTriplePattern {

    public ConvertTriplePattern(){}

    public static ArrayList<String> convert(Triple triple) {
        return convert(null, triple);
    }

    public static ArrayList<String> convert(OpStream op, Triple triple) {
        if(op != null) triple = op.getBasicPattern().get(0);

        String subject = null;
        String predicate = null;
        String object = null;

        ArrayList<String> variables = null;

        if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";
            predicate = "\""+triple.getPredicate().toString()+"\"";
            object = "\""+triple.getObject().toString()+"\"";
            variables = new ArrayList(Arrays.asList(new String[]{subject, predicate, object}));
        } else if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";
            predicate = "\""+triple.getPredicate().toString()+"\"";
            variables = new ArrayList(Arrays.asList(new String[]{subject, predicate}));
        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";
            variables = new ArrayList(Arrays.asList(new String[]{subject}));
        } else if(triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            subject = "\""+triple.getSubject().toString()+"\"";
            object = "\""+triple.getObject().toString()+"\"";
            variables = new ArrayList(Arrays.asList(new String[]{subject, object}));
        } else if(!triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            predicate = "\""+triple.getPredicate().toString()+"\"";
            object = "\""+triple.getObject().toString()+"\"";
            variables = new ArrayList(Arrays.asList(new String[]{predicate, object}));
        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && triple.getObject().isVariable()) {
            object = "\""+triple.getObject().toString()+"\"";
            variables = new ArrayList(Arrays.asList(new String[]{object}));
        } else if(!triple.getSubject().isVariable() && !triple.getPredicate().isVariable() && !triple.getObject().isVariable()) {
            variables = null;
        }
        return variables;
    }
}
