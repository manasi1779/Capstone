public class TagGenerator {

    public static TagGenerator tagGenerator;
    static int count;


    public TagGenerator(){
        if(tagGenerator == null){
            tagGenerator = new TagGenerator();
        }
    }

    public TagGenerator getTagGenerator(){
        return tagGenerator;
    }

    public String getNextAlias(){
        count++;
        return ""+('a'+count);
    }
}
