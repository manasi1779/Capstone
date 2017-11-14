package metadata;

public class Group {

    public String groupLabel;
    public String property;
    public AggrOperator operator;

    public Group(String label, String property, AggrOperator operator){
        groupLabel = label;
        this.property = property;
        this.operator = operator;

    }

}
