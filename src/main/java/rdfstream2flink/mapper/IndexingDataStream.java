package rdfstream2flink.mapper;

import com.hp.hpl.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexingDataStream {

    private static HashMap<Integer, List<Var>> solutionEnvsMapping = new HashMap<>();
    public static int indice = 0;

    public static void insertSolutionMapping(Integer indice_sm, List<Var> varsProjection) {
        solutionEnvsMapping.put(indice_sm, varsProjection);
        indice++;
    }

    public static int getIndice(){
        return indice;
    }

    public static HashMap<Integer, List<Var>> getSolutionEnvsMapping(){
        return solutionEnvsMapping;
    }

    @Override
    public String toString() {
        String sm = "";
        for (Map.Entry<Integer, List<Var>> hm : solutionEnvsMapping.entrySet()) {
            sm += hm.getKey() + "-->" + hm.getValue().toString() + "\t";
        }
        return sm;
    }
}