package rdfstream2flink.mapper;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.deri.cqels.engine.*;
import org.deri.cqels.lang.cqels.OpStream;
import org.w3c.dom.ranges.Range;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ConvertLQP2FlinkProgram extends OpVisitorBase {

    private static String flinkProgram = "";
    private static Map<Integer, Object> windows = new HashMap<Integer, Object>();

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
    public void visit(OpFilter opFilter) {
        ExprList exprList = opFilter.getExprs();
        opFilter.getSubOp().visit(this);
        for ( Expr expression : exprList ) {
            flinkProgram += "\t\tDataStream<SolutionMapping> sm"+(SolutionMapping.getIndiceSM())+" = sm"+(SolutionMapping.getIndiceSM()-1)+"\n" +
                    "\t\t\t.filter(new Filter(\""+FilterConvert.convert(expression)+"\"));\n\n";

            ArrayList<String> variables = SolutionMapping.getSolutionMapping().get(SolutionMapping.getIndiceSM()-1);

            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndiceSM(), variables);
        }
    }

    @Override
    public void visit(OpQuadPattern quadPattern) {
        visit((OpStream) quadPattern);
    }

    public void visit(Object op, Boolean beginJoin) {
        if(beginJoin && op instanceof OpStream) {
            flinkProgram += getRDFStream(((OpStream) op).getGraphNode().toString());
            //flinkProgram += ConvertTriplePattern.convert(((OpStream) op), null, this);
            ArrayList<String> variables = ConvertTriplePattern.convert(((OpStream) op), null);
            //flinkProgram += generateOpStream(((OpStream) op), Boolean.FALSE, variables);
            generateOpStream(((OpStream) op), Boolean.FALSE, variables);
            windows.put(SolutionMapping.getIndiceSM(), ((OpStream) op).getWindow());
        }
        else {
            ((Op) op).visit(this);
        }
    }

    public void visit(OpStream op) {
        //flinkProgram += generateOpStream(op, Boolean.TRUE, null);
        generateOpStream(op, Boolean.TRUE, null);
    }

    public void generateOpStream(OpStream op, Boolean createDS, ArrayList<String> variables) {
        if(createDS) flinkProgram += getRDFStream(op.getGraphNode().toString());

        Triple t = op.getBasicPattern().get(0);

        flinkProgram += "\t\tDataStream<SolutionMapping> sm" + SolutionMapping.getIndiceSM() +
                " = rdfStream" + SolutionMapping.getIndiceDS() + "\n" +
                ((SolutionMapping.getTypeTime().equals("E")) ?
                        "\t\t\t.assignTimestampsAndWatermarks(new TimestampExtractor())\n":"") +
                "\t\t\t.keyBy(new WindowKeySelector("+
                "\""+t.getSubject().toString()+"\", " +
                "\""+t.getPredicate().toString()+"\", " +
                "\""+evalObject(t.getObject())+"\"))\n";
        if (op.getWindow() instanceof TripleWindow) {
            //Triple t = op.getBasicPattern().get(0);
            try {
                Long triplesNumber = (Long) getField("t", op.getWindow());
                flinkProgram += "\t\t\t.countWindow("+triplesNumber+")\n" +
                        "\t\t\t.process(new Triple2SolutionMapping2(" +
                        "\""+t.getSubject().toString()+"\", " +
                        "\""+t.getPredicate().toString()+"\", " +
                        "\""+evalObject(t.getObject())+"\"));\n\n";
                SolutionMapping.insertSolutionMapping(SolutionMapping.getIndiceSM(), variables);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        else if (op.getWindow() instanceof RangeWindow) {
            //Triple t = op.getBasicPattern().get(0);
            RangeWindow w = (RangeWindow) op.getWindow();
            flinkProgram += getRangeWindow(w) +
                    "\t\t\t.apply(new Triple2SolutionMapping3(" +
                    "\""+t.getSubject().toString()+"\", " +
                    "\""+t.getPredicate().toString()+"\", " +
                    "\""+evalObject(t.getObject())+"\"));\n\n";
            SolutionMapping.insertSolutionMapping(SolutionMapping.getIndiceSM(), variables);
        }

        //return solW;
    }

    public void visit(OpJoin opJoin) {
        Op opLeft = opJoin.getLeft();
        visit(opLeft, true);
        int indice_sm_left = SolutionMapping.getIndiceSM()-1;
        Object lW = windows.get(SolutionMapping.getIndiceSM());

        Op opRight = opJoin.getRight();
        visit(opRight, true);
        int indice_sm_right = SolutionMapping.getIndiceSM()-1;
        Object rW = windows.get(SolutionMapping.getIndiceSM());

        int indice_sm_join = SolutionMapping.getIndiceSM();

        ArrayList<String> listKeys = SolutionMapping.getKey(indice_sm_left, indice_sm_right);

        if(listKeys.size()>0) {
            if((lW instanceof TripleWindow) || (rW instanceof TripleWindow)) {
                String keys = JoinKeys.keys(listKeys);
                try {
                    flinkProgram += "\t\tDataStream<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                            "\t\t\t.where(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                            "\t\t\t.equalTo(new JoinKeySelector(new String[]{" + keys + "}))\n" +
                            "\t\t\t.window(GlobalWindows.create())\n" +
                            "\t\t\t.trigger(CountTrigger.of("+(getField("t", (lW!=null) ? lW : rW).toString())+"))\n" +
                            "\t\t\t.apply(new Join());" +
                            "\n\n";
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            } else if((lW instanceof RangeWindow) || (rW instanceof RangeWindow)) {
                String keys = JoinKeys.keys(listKeys);
                RangeWindow w = (RangeWindow) ((lW!=null) ? lW : rW);
                flinkProgram += "\t\tDataStream<SolutionMapping> sm" + indice_sm_join + " = sm" + indice_sm_left + ".join(sm" + indice_sm_right + ")\n" +
                        "\t\t\t.where(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                        "\t\t\t.equalTo(new JoinKeySelector(new String[]{"+keys+"}))\n" +
                        getRangeWindow(w) +
                        /*((w.getSlide()>0) ?
                                ("\t\t\t.window(SlidingProcessingTimeWindows.of(" + getTime(w.getDuration()) +  ", "+getTime(w.getSlide())+"))\n") :
                                ("\t\t\t.window(TumblingProcessingTimeWindows.of("+ getTime(w.getDuration())+"))\n")) +*/
                        "\t\t\t.apply(new Join());\n\n";
            }
        }

        SolutionMapping.join(indice_sm_join, indice_sm_left, indice_sm_right);
    }

    private String getRangeWindow(RangeWindow w) {
        if (SolutionMapping.getTypeTime().equals("E")) {
            return (w.getSlide()>0) ?
                    ("\t\t\t.window(SlidingEventTimeWindows.of(" + getTime(w.getDuration()) +  ", "+getTime(w.getSlide())+"))\n") :
                    ("\t\t\t.window(TumblingEventTimeWindows.of("+ getTime(w.getDuration())+"))\n");
        }
        return (w.getSlide()>0) ?
                ("\t\t\t.window(SlidingProcessingTimeWindows.of(" + getTime(w.getDuration()) +  ", "+getTime(w.getSlide())+"))\n") :
                ("\t\t\t.window(TumblingProcessingTimeWindows.of("+ getTime(w.getDuration())+"))\n");
    }

    public static String getRDFStream(String uri) {
        String host = uri;
        Integer port = null;
        String values[] = uri.split(":");
        if(values.length==2) {
            host = values[0];
            port = Integer.parseInt(values[1]);
        }
        if(!flinkProgram.contains(String.format("LoadRDFStream.fromSocket(env, \"%s\", %s)", host, port))) {
            return String.format(
                    "\t\t//************ Applying Transformations To rdfStream%s ************\n" +
                            "\t\tDataStream<TripleTS> rdfStream%s = LoadRDFStream.fromSocket(env, \"%s\", %s);\n\n",
                    SolutionMapping.incrementIDS(), SolutionMapping.getIndiceDS(), host, port);
        }
        return "";
    }

    public static String evalObject(Node node){
        if(node.isLiteral()) {
            return node.toString().replace("\"", "\\\"");
        }
        return node.toString();
    }

    public static String getFlinkProgram(){
        return flinkProgram;
    }

    private String getTime(long t) {
        t /= 1E6;
        return "Time.of(" + t + ", TimeUnit.MILLISECONDS)";
    }

    public Object getField(String fieldName, Object obj) throws NoSuchFieldException, IllegalAccessException {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
}
