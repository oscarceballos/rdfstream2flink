package rdfstream2flink.runner;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.sql.Timestamp;

public class TripleTS extends Triple {
    private Long timeStamp;

    public TripleTS(Node s, Node p, Node o, Long timeStamp) {
        super(s, p, o);
        this.timeStamp = (timeStamp==null) ? (new Timestamp(System.currentTimeMillis())).getTime() : timeStamp;
    }

    public TripleTS(Triple t, Long timeStamp) {
        super(t.getSubject(), t.getPredicate(), t.getObject());
        this.timeStamp = (timeStamp==null) ? (new Timestamp(System.currentTimeMillis())).getTime() : timeStamp;
    }

    public TripleTS(Node s, Node p, Node o) {
        super(s, p, o);
        this.timeStamp = (new Timestamp(System.currentTimeMillis())).getTime();
    }

    public TripleTS(Triple t) {
        super(t.getSubject(), t.getPredicate(), t.getObject());
        this.timeStamp = (new Timestamp(System.currentTimeMillis())).getTime();
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "TripleTS{" + super.toString() +
                ", timeStamp=" + timeStamp + '}';
    }
}
