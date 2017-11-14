package metadata;

public class Edge{
    public LabelPattern from;
    LabelPattern to;
    String relationShipType;

    public Edge(LabelPattern from, LabelPattern to){
        this.from = from;
        this.to = to;
    }

    public String toString(){
        return "("+from.token+":"+from.name+")"+"--"+"("+to.token+":"+to.name+")";
    }

    public String plainString(){
        return "("+from.token+")"+"--"+"("+to.token+")";
    }
}
