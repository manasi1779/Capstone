package metadata;

public class AggrOperator {
    public String operator;
    public String label, property;

    public AggrOperator(String operator, String label, String property){
        this.operator = operator;
        this.label = label;
        this.property = property;
    }

    @Override
    public String toString(){
        return operator+"("+label+"."+property+")";
    }

    public String getLabel(){
        return operator+"_"+label+"_"+property;
    }
}
