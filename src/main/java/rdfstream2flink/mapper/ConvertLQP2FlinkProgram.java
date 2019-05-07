package rdfstream2flink.mapper;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.*;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import org.deri.cqels.engine.RangeWindow;
import org.deri.cqels.engine.TripleWindow;
import org.deri.cqels.lang.cqels.OpStream;
import rdfstream2flink.mapper.JoinKeys;
import rdfstream2flink.mapper.SolutionMapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {

    private static String flinkProgram = "";

    public  ConvertLQP2FlinkProgram() {
        super();
    }

    /*@Override
    public void visit(OpBGP opBGP) {
        List<Triple> listTriplePatterns = opBGP.getPattern().getList();
        flinkProgram += ConvertTriplePatternGroup.convert(listTriplePatterns);
    }*/

    public void convertTripleWindow(OpStream opStream){

        Field field = null;
        Long triplesNumber = null;

        TripleWindow tripleWindow = (TripleWindow) opStream.getWindow();
        try {
            field = tripleWindow.getClass().getDeclaredField("t");
            field.setAccessible(true);
            triplesNumber = field.getLong(tripleWindow);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        List<Triple> listTriplePatterns = opStream.getBasicPattern().getList();
        flinkProgram += ConvertTriplePatternGroup.convert(listTriplePatterns, triplesNumber, 'T');
    }

    public void visit(OpQuadPattern opQuadPattern){
        //System.out.println("es TripleWindow");
        OpStream opStream = (OpStream) opQuadPattern;
        if (opStream.getWindow().getClass() == TripleWindow.class) {
            convertTripleWindow(opStream);

        } else if (opStream.getWindow().getClass() == RangeWindow.class) {
            //System.out.println("es RangeWindow");
            RangeWindow rangeWindow = (RangeWindow) opStream.getWindow();
            System.out.println("Duration: "+(rangeWindow.getDuration()/1000000)+"\n"+
                    "Sile: "+(rangeWindow.getSlide()/1000000));
        }

        List<Quad> quads = opQuadPattern.getPattern().getList();
        System.out.println(quads.get(0).getGraph());

    }

    @Override
    public void visit(OpJoin opJoin) {
        Op opLeft = opJoin.getLeft();
        Op opRight = opJoin.getRight();

        opLeft.visit(this);
        int indice_sm_left = SolutionMapping.getIndice()-1;

        opRight.visit(this);
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

    /*@Override
    public void visit(OpLeftJoin opLeftJoin) {
        Op opLeft = opLeftJoin.getLeft();
        Op opRight = opLeftJoin.getRight();

        opLeft.visit(this);
        int indice_sm_left = SolutionMapping.getIndice()-1;

        opRight.visit(this);
        int indice_sm_right = SolutionMapping.getIndice()-1;

        int indice_sm_join = SolutionMapping.getIndice();

        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);

        if(listKeys.size()>0) {
            String keys = JoinKeys.keys(listKeys);
            flinkProgram += "\t\tDataSet<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".leftOuterJoin(sm" + indice_sm_right + ")\n" +
                    "\t\t\t.where(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.equalTo(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                    "\t\t\t.with(new LeftJoin());" +
                    "\n\n";
        }
        else {
            flinkProgram += "\t\tDataSet<SolutionMapping> sm"+indice_sm_join+" = sm"+indice_sm_left+".cross(sm"+indice_sm_right+")\n" +
                    "\t\t\t.with(new Cross());" +
                    "\n\n";
        }

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);

        if(opLeftJoin.getExprs() != null) {
            this.visit(opLeftJoin.getExprs());
        }
    }*/

    /*@Override
    public void visit(OpUnion opUnion) {
        Op opLeft = opUnion.getLeft();
        Op opRight = opUnion.getRight();

        opLeft.visit(this);
        int indice_sm_left = SolutionMapping.getIndice()-1;

        opRight.visit(this);
        int indice_sm_right = SolutionMapping.getIndice()-1;

        int indice_sm_join = SolutionMapping.getIndice();

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+indice_sm_join+" = sm"+indice_sm_left+".union(sm"+indice_sm_right+");" +
                "\n\n";

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
    }*/

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

        flinkProgram += "\t\tDataStream<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.map(new Project(new String[]{"+varsProject+"}));\n\n";

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }

    /*@Override
    public void visit(OpFilter opFilter) {
        ExprList exprList = opFilter.getExprs();
        opFilter.getSubOp().visit(this);
        for ( Expr expression : exprList ) {
            flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                    "\t\t\t.filter(new Filter(\""+FilterConvert.convert(expression)+"\"));\n\n";

            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
        }
    }*/

    /*public void visit(ExprList exprList) {
        for ( Expr expression : exprList ) {
            flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                    "\t\t\t.filter(new Filter(\""+FilterConvert.convert(expression)+"\"));\n\n";

            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
        }
    }*/

    /*@Override
    public void visit(OpDistinct opDistinct) {
        opDistinct.getSubOp().visit(this);

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.distinct(new DistinctKeySelector());\n\n";

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }*/

    /*@Override
    public void visit(OpOrder opOrder) {
        List<SortCondition> sortCondition = opOrder.getConditions();

        String order="";
        if(sortCondition.get(0).getDirection()==-2) {
            order = "Order.ASCENDING";
        } else if (sortCondition.get(0).getDirection()==-1) {
            order = "Order.DESCENDING";
        }

        opOrder.getSubOp().visit(this);

        Expr expression = sortCondition.get(0).getExpression();

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+SolutionMapping.getIndice()+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t\t\t.sortPartition(new OrderKeySelector(\""+expression+"\"), "+order+")\n" +
                "\t\t\t\t\t.setParallelism(1);\n" +
                "\t\n";

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }*/


    /*@Override
    public void visit(OpSlice opSlice) {
        opSlice.getSubOp().visit(this);

        flinkProgram += "\t\tDataSet<SolutionMapping> sm"+(SolutionMapping.getIndice())+" = sm"+(SolutionMapping.getIndice()-1)+"\n" +
                "\t\t\t.first("+opSlice.getLength()+");\n\n";

        ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndice()-1);

        SolutionMapping.insertSolutionMapping(SolutionMapping.getIndice(), variables);
    }*/

    public static String getFlinkProgram(){
        return flinkProgram;
    }
}
