public class Where{

    String labelToken;
    String property;
    Object value;
    String operator;

    public Where(String labelToken, String property, String operator, Object value){
        this.labelToken = labelToken;
        this.property = property;
        this.operator = operator;
        this.value = value;
    }

    public String toString(){
        if(value instanceof String)
            return labelToken+"."+property+operator+"\""+value+"\"";
        else
            return labelToken+"."+property+operator+value;
    }

    public Object performOperation(Object actualValue){
        Object result = null;
        switch(operator){
            case "=":{
                result = actualValue.equals(value);
                break;
            }
            case "!=":{
                result = !actualValue.equals(value);
                break;
            }
            case "<":{
                result = ((Comparable)actualValue).compareTo((Comparable)value) < 0;
                break;
            }
            case ">":{
                result = ((Comparable)actualValue).compareTo((Comparable)value) > 0;
                break;
            }
            case "<=":{
                result = ((Comparable)actualValue).compareTo((Comparable)value) < 0 || actualValue.equals(value);
                break;
            }
            case ">=":{
                result = ((Comparable)actualValue).compareTo((Comparable)value) > 0 || actualValue.equals(value);
                break;
            }
        }
        return result;
    }

}
