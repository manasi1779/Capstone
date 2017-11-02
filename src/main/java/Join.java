public class Join {
    String db1, db2;
    String labelToken1, labelToken2;
    String property1, property2;
    String operator;

    public Join(String db1, String db2, String label1, String property1,  String label2, String property2,String operator){
        this.db1 = db1;
        this.db2 = db2;
        labelToken1 = label1;
        labelToken2 = label2;
        this.property1 = property1;
        this.property2 = property2;
        this.operator = operator;
    }

    public String toString(){
        return db1+"."+labelToken1+"."+property1+operator+db2+"."+labelToken2+"."+property2;
    }

    public boolean performOperation(Object value1, Object value2){
        boolean result = false;
        switch(operator){
            case "=":{
                result = value1.equals(value2);
                break;
            }
            case "!=":{
                result = !value1.equals(value2);
                break;
            }
            case "<":{
                result = ((Comparable)value1).compareTo((Comparable)value2) < 0;
                break;
            }
            case ">":{
                result = ((Comparable)value1).compareTo((Comparable)value2) > 0;
                break;
            }
            case "<=":{
                result = ((Comparable)value1).compareTo((Comparable)value2) < 0 || value1.equals(value2);
                break;
            }
            case ">=":{
                result = ((Comparable)value1).compareTo((Comparable)value2) > 0 || value1.equals(value2);
                break;
            }
        }
        return result;
    }

}
