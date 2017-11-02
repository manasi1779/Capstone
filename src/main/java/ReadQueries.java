import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


public class ReadQueries {

    public void parseEdges(ArrayList<Where> wheres){
        HashMap<String, LabelPattern> labels = new HashMap<>();
        ArrayList<Edge> edges = new ArrayList<>();
        ArrayList<String> queryNodes = new ArrayList<>();
        ReadQueries read = new ReadQueries();
        Scanner s = null;
        try {
            s = new Scanner(new File("query1.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String current = "";

        while(s.hasNextLine()){
            String token = s.nextLine();
            String tokens[] = token.split(" ");

            if(tokens[0].equalsIgnoreCase("select")){
                current = "select";
                continue;
            }else if(tokens[0].equalsIgnoreCase("match")){
                current = "match";
                continue;
            }else if(tokens[0].equalsIgnoreCase("where")){
                current = "where";
                continue;
            }

            //If this is a new alias add it to labels
            if(current.equals("select")){
                LabelPattern label = new LabelPattern(tokens[0].trim(), tokens[1].trim(), null);
                labels.put(tokens[0].trim(), label);
                queryNodes.add(tokens[0].trim());
            }
            else if(current.equals("match")){
                edges.add(new Edge(labels.get(tokens[0]),labels.get(tokens[1])));
            }
            else if(current.equals("where")){
                if(token.contains("&&")){
                    tokens = token.split("&&");
                }else if(token.contains("||")){
                    tokens = token.split("||");
                }else{


                }

                for(String predicate: tokens){
                   String parsed[] = predicate.split(" ");
                    Where where = new Where(parsed[0], parsed[1], parsed[2],parsed[3]);
                    wheres.add(where);
                }
            }
        }
        s.close();
    }

}


