package rdfstream2flink.mapper;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Var;
import org.deri.cqels.engine.*;
import org.deri.cqels.lang.cqels.OpStream;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {

    private static String flinkProgram = "";

    @Override
    public void visit(OpProject opProject) {
        ArrayList<String> variables = new ArrayList<>();

        String varsProject = "";
        Iterator<Var> iter = opProject.getVars().iterator();
        for (; iter.hasNext(); ) {
            String var = "\"?"+iter.next().getVarName()+"\"";
            varsProject += var;
            if(iter.hasNext()){
                varsProject += ", ";
            }
            variables.add(var);
        }

        opProject.getSubOp().visit(this);

        flinkProgram += "\t\tDataStream<SolutionMapping> sm"+(SolutionMapping.getIndiceSM())+" = sm"+(SolutionMapping.getIndiceSM()-1)+"\n" +
                "\t\t\t.map(new Project(new String[]{"+varsProject+"}));\n\n";

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndiceSM(), variables);
    }

    @Override
    public void visit(OpQuadPattern quadPattern) {
        visit((OpStream) quadPattern);
    }

    public void visit(Object op, Boolean beginJoin) {
        if(beginJoin && op instanceof OpStream) {
            flinkProgram += "\t\tDataStream<SolutionMapping> sm" + SolutionMapping.getIndiceSM() + " = rdfStream"+SolutionMapping.getIndiceDS()+"\n" +
                    ConvertTriplePattern.convert(((OpStream) op).getBasicPattern().get(0), SolutionMapping.getIndiceSM());
        }
        else {
            ((Op) op).visit(this);
        }
    }

    public void visit(OpStream op) {
        flinkProgram += String.format(
                "\t\t//************ Applying Transformations To rdfStream%s ************\n",
                SolutionMapping.incrementIDS());
        flinkProgram += String.format(
                "\t\tDataStream<Triple> rdfStream%s = LoadRDFStream.fromSocket(env, \"%s\", %s);\n\n",
                SolutionMapping.getIndiceDS(), /*op.getGraphNode().toString()*/"localhost", 5555);

        if (op.getWindow() instanceof TripleWindow) {
            try {
                Long triplesNumber = (Long) getField("t", op.getWindow());
                List<Triple> listTriplePattern = op.getBasicPattern().getList();
                flinkProgram += ConvertTriplePatternGroup.convert(listTriplePattern, triplesNumber, 'T');
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    public void visit(OpJoin opJoin) {
        Op opLeft = opJoin.getLeft();
        visit(opLeft, true);
        int indice_sm_left = SolutionMapping.getIndiceSM()-1;

        Op opRight = opJoin.getRight();
        visit(opRight, true);
        int indice_sm_right = SolutionMapping.getIndiceSM()-1;

        int indice_sm_join = SolutionMapping.getIndiceSM();

        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);

        if(listKeys.size()>0) {
            String keys = JoinKeys.keys(listKeys);
            flinkProgram += "\t\tDataStream<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.where(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))\n" +
                    "\t\t\t.apply(new Join());" +
                    "\n\n";
        }

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
    }

    public static String getFlinkProgram(){
        return flinkProgram;
    }

    public Object getField(String fieldName, Object obj) throws NoSuchFieldException, IllegalAccessException {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
}
