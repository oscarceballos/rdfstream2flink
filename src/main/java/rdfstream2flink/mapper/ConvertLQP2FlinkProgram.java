package rdfstream2flink.mapper;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import org.deri.cqels.engine.*;
import org.deri.cqels.lang.cqels.OpStream;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ConvertLQP2FlinkProgram implements RouterVisitor {

    private static String flinkProgram = "";


    @Override
    public void visit(JoinRouter router) {
        System.out.println("JoinRouter router");
    }

    @Override
    public void visit(IndexedTripleRouter router) {
        System.out.println("IndexedTripleRouter router");
    }

    @Override
    public void visit(ProjectRouter router) {
        System.out.println("ProjectRouter router");
    }

    @Override
    public void visit(ThroughRouter router) {
        System.out.println("ThroughRouter router");

        try {
            List<IndexedTripleRouter> dataflows = (List<IndexedTripleRouter>) getField("dataflows", router);
            for (IndexedTripleRouter iter : dataflows) {
                if(iter.getOp() instanceof OpStream) visit((OpStream) iter.getOp());
            }
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void visit(BDBGraphPatternRouter router) {
        System.out.println("BDBGraphPatternRouter router");
    }

    @Override
    public void visit(ExtendRouter router) {
        System.out.println("ExtendRouter router");
    }

    @Override
    public void visit(FilterExprRouter router) {
        System.out.println("FilterExprRouter router");
    }

    @Override
    public void visit(ContinuousConstruct router) {
        System.out.println("ContinuousConstruct router");
    }

    @Override
    public void visit(ContinuousSelect router) {
        System.out.println("ContinuousSelect router");
    }

    @Override
    public void visit(GroupRouter router) {
        System.out.println("GroupRouter router");
    }

    @Override
    public void visit(OpRouter router) {
        System.out.println("OpRouter router");
    }

    public void visit(OpStream op) {
        if (op.getWindow() instanceof TripleWindow) visit((TripleWindow) op.getWindow());
        else if (op.getWindow() instanceof RangeWindow) visit((RangeWindow) op.getWindow());
        op.visit(new OpVisitorBase());
    }

    public void visit(TripleWindow window) {
        System.out.println(window);
    }

    public void visit(RangeWindow window) {
        System.out.println(window);
    }

    public void visit(OpJoin opJoin) {
        visit(opJoin.getLeft(), opJoin.getRight());
    }

    public void visit(Op opLeft, Op opRight) {

        opLeft.visit(new OpVisitorBase());
        int indice_sm_left = SolutionMapping.getIndice()-1;

        opRight.visit(new OpVisitorBase());
        int indice_sm_right = SolutionMapping.getIndice()-1;

        int indice_sm_join = SolutionMapping.getIndice();

        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);

        if(listKeys.size()>0) {
            String keys = JoinKeys.keys(listKeys);
            flinkProgram += "\t\tDataStream<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.where(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))\n" +
                    "\t\t\t.apply(new Join());" +
                    "\n\n";
        } else {
            String keys = JoinKeys.keys(listKeys);
            flinkProgram = "\t\tDataStream<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".coGroup(sm" + indice_sm_right + ")\n" +
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
