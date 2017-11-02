public class Tag {

    TagGenerator tg = new TagGenerator();

    String label;
    String[] attribute;
    Object[] value;

    public String toString(){
        String query = tg.getNextAlias()+":"+label;
        query += "{";
        for(int i = 0; i < attribute.length; i++){
            query += attribute[i];
            if(value[i] instanceof Object[]){
                query += "in [";
                value[i] = (Object[])value[i];
                for(int j = 0; j < ((Object[])value[i]).length; j++){
                    query += ((Object[])value[i])[j]+",";
                }
                query = query.substring(0, query.length()-1);
                query += "]";
            }
            else
                query += ":"+value[i];
            query += "}";
        }
        return query;
    }

}
