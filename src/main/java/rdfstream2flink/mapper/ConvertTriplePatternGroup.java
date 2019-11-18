package rdfstream2flink.mapper;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import java.util.ArrayList;
import java.util.List;

public class ConvertTriplePatternGroup {

    public ConvertTriplePatternGroup() { }

    public static String evalObject(Node node){
        if(node.isLiteral()) {
            return node.toString().replace("\"", "\\\"");
        }
        return node.toString();
    }

    public static String joinSolutionMapping (int indice_sm_join, int indice_sm_left, int indice_sm_right, Character window) {
        String sm = "";
        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);
        switch (window) {
            case 'T':
                if (listKeys.size() > 0) {
                    String keys = JoinKeys.keys(listKeys);
                    sm += "\t\tDataStream<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                            "\t\t\t.where(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                            "\t\t\t.equalTo(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                            "\t\t\t.window(GlobalWindows.create())\n" +
                            "\t\t\t.trigger(CountTrigger.of(1))\n" +
                            "\t\t\t.apply(new Join());" +
                            "\n\n";
                }
                break;
            case 'R':
                if (listKeys.size() > 0) {
                    String keys = JoinKeys.keys(listKeys);
                    sm += "\t\tDataStream<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                            "\t\t\t.where(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                            "\t\t\t.equalTo(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                            "\t\t\t.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))\n" +
                            "\t\t\t.apply(new Join());" +
                            "\n\n";
                }
                break;
        }

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
        return sm;
    }

    public static String convertTPG(List<Triple> listTriplePatterns, int indiceLTP, int count, int indiceSM, String bgp, Long triplesNumber, Character window) {
        if (indiceLTP>=listTriplePatterns.size() && count==1) {
            return bgp;
        } else {
            if(count == 2) {
                bgp += joinSolutionMapping(indiceSM,indiceSM-2, indiceSM-1, window);
                count = 1;
            } else {
                if(listTriplePatterns.size()==1) {
                    Triple t = listTriplePatterns.get(0);
                    bgp = "\t\tDataStream<SolutionMapping> sm" + indiceSM + " = rdfStream"+SolutionMapping.getIndiceDS()+"\n" +
                            "\t\t\t.keyBy(new WindowKeySelector())\n" +
                            "\t\t\t.countWindow("+triplesNumber+")\n" +
                            "\t\t\t.process(new Triple2SolutionMapping2(" +
                            "\""+t.getSubject().toString()+"\", " +
                            "\""+t.getPredicate().toString()+"\", " +
                            "\""+evalObject(t.getObject())+"\"));\n" +
                            "\n";
                    SolutionMapping.insertSolutionMapping(indiceSM, null);
                } else {
                    bgp += "\t\tDataStream<SolutionMapping> sm" + indiceSM + " = rdfStream"+SolutionMapping.getIndiceDS()+"\n" +
                            ConvertTriplePattern.convert(listTriplePatterns.get(indiceLTP), indiceSM);
                }
                indiceLTP += 1;
                count += 1;
            }
            return convertTPG(listTriplePatterns, indiceLTP, count, SolutionMapping.getIndiceSM(), bgp, triplesNumber, window);
        }
    }

    public static String convert(List<Triple> listTriplePatterns, Long triplesNumber, Character window){
        return convertTPG(listTriplePatterns, 0, 0, SolutionMapping.getIndiceSM(), "", triplesNumber, window);
    }
}
