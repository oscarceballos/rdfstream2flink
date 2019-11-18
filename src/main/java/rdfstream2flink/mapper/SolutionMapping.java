package rdfstream2flink.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SolutionMapping {

    private static HashMap<Integer, ArrayList<String>> solutionMapping = new HashMap<>();
    private static String typeTime = "E"; // For default is used Event Time
    private static int indiceSM = 1;
    private static int indiceDS = 0;

    public static ArrayList<String> getKey(int indice_sm_left, int indice_sm_right){
        ArrayList<String> listKeys = new ArrayList<>();
        for (String key_left : solutionMapping.get(indice_sm_left)) {
            for (String key_right : solutionMapping.get(indice_sm_right)) {
                if (key_left.equals(key_right)) {
                    listKeys.add(key_left);
                }
            }
        }
        return listKeys;
    }

    public static void join(int indice_sm, int indice_sm_left, int indice_sm_right){
        ArrayList<String> variables = new ArrayList<>();

        for (String varSML : solutionMapping.get(indice_sm_left)) {
            variables.add(varSML);
        }

        for (String varSMR : solutionMapping.get(indice_sm_right)) {
            if (!(variables.contains(varSMR))) {
                variables.add(varSMR);
            }
        }
        insertSolutionMapping(indice_sm, variables);
    }

    public static void insertSolutionMapping(Integer indice_sm, ArrayList<String> variables){
        solutionMapping.put(indice_sm, variables);
        indiceSM++;
    }

    public static int incrementISM() {
        indiceSM++;
        return indiceSM;
    }

    public static int getIndiceSM(){
        return indiceSM;
    }

    public static int getIndiceDS(){
        return indiceDS;
    }

    public static int incrementIDS() {
        indiceDS++;
        return indiceDS;
    }

    public static HashMap<Integer, ArrayList<String>> getSolutionMapping(){
        return solutionMapping;
    }

    public static String getTypeTime() {
        return typeTime;
    }

    public static void setTypeTime(String typeTime) {
        SolutionMapping.typeTime = typeTime;
    }

    @Override
    public String toString() {
        String sm="";
        for (Map.Entry<Integer, ArrayList<String>> hm : solutionMapping.entrySet()) {
            sm += hm.getKey() + "-->" + hm.getValue().toString() + "\t";
        }
        return sm;
    }
}
