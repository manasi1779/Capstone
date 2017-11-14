package graphql;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;

public class NewLabel {

    int count = 0;
    static ArrayList<String> newLabels = new ArrayList<>();
    private NewLabel() {}

    private static class SingletonHolder {
        private static final NewLabel INSTANCE = new NewLabel();
    }

    public static NewLabel getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public String getNextLabel(){
        count++;
        newLabels.add("Graph"+count);
        return "Graph"+count;
    }

    public void tearDown(GraphDatabaseService db){
        for(String key: newLabels){
            String cypherQuery = "Match ( x :"+key+") Remove x :"+key;
            System.out.println(cypherQuery);
            Transaction tx = db.beginTx();
            db.execute(cypherQuery);
            tx.success();
            tx.close();
        }
    }
}
