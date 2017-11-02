import java.util.ArrayList;

class LabelPattern{
    String token, name;
    ArrayList<String>  properties = new ArrayList();

    public LabelPattern(String token, String name, ArrayList<String>  properties){
        this.token = token;
        this.name = name;
        this.properties = properties;
    }

    public String toString(){
        return token+":"+name;
    }

    @Override
    public int hashCode(){
        return (token+":"+name).hashCode();
    }

    @Override
    public boolean equals(Object other){
        return toString().equals(other.toString());
    }
}
