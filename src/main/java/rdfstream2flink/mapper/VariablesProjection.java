package rdfstream2flink.mapper;

import com.hp.hpl.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.List;

public class VariablesProjection {
    private static List<Var> vars = new ArrayList<>();

    public static List<Var> getVars() {
        return vars;
    }

    public static void setVars(List<Var> vars) {
        VariablesProjection.vars = vars;
    }

    public static void projectVars() {
        ArrayList<String> variables = new ArrayList<>();
        String varsProject = "";

        for (Var iter : vars) {
            String var = "\"?"+iter.getVarName()+"\"";
            if(!varsProject.equals(""))
                varsProject += ",";
            varsProject += var;
            variables.add(var);
        }

        ConvertLQP2FlinkProgram.addToFlinkProgram("\t\tDataStream<SolutionMapping> sm"+
                (SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.map(new Project(new String[]{"+varsProject+"}));\n\n");

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }
}
