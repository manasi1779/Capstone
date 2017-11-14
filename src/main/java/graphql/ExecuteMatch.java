package graphql;

import metadata.LabelPattern;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import parser.Parser;

public class ExecuteMatch {

    @Autowired
    UpdateCypher updateCypher;

    public void constructMatchSetReturn(GraphDatabaseService db, Parser parser){
        String cypherQuery = updateCypher.addMatch(parser.edges);
        // put in labelsMap new label and set it to result as well
        cypherQuery += " Set ";
        NewLabel labelMaker = NewLabel.getInstance();
        for(LabelPattern label: parser.labels[0]){
            String nextlabel = labelMaker.getNextLabel();
            cypherQuery += label.token+":"+nextlabel+",";
            LabelPattern labelPattern = parser.labelsMap.get(label.token);
            labelPattern.name = nextlabel;
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1);
        System.out.println(cypherQuery);
        db.execute(cypherQuery);
    }
}
