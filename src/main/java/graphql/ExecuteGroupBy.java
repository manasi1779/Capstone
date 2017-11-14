package graphql;

import metadata.LabelPattern;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import parser.Parser;

import java.util.HashMap;
import java.util.Map;

public class ExecuteGroupBy {

    @Autowired
    UpdateCypher updateCypher;

    public void groupBy(GraphDatabaseService db, Parser parser){
        HashMap<String, String> groupLabels = new HashMap<>();
        // Add new label to each node with value of group clause
        String distinctQuery = "MATCH (node:"+parser.labelsMap.get(parser.groupBy.groupLabel).name+") return distinct node."+parser.groupBy.property;
        Transaction tx = db.beginTx();
        Result result = db.execute(distinctQuery);
        String query = updateCypher.addMatch(parser.edges);
        NewLabel labelMaker = NewLabel.getInstance();
        while (result.hasNext()){
            String nextLabel = labelMaker.getNextLabel();
            Map<String, Object> res = result.next();
            String node = res.get("node."+parser.groupBy.property).toString();
            if (node.contains("\""))
                continue;
            System.out.println(node);
            String groupQuery = query;
            groupQuery += " WHERE "+parser.groupBy.groupLabel+"."+
                    parser.groupBy.property+"= \""+node+"\"";
            groupQuery += " WITH "+parser.groupBy.operator+" AS aggrValue, ";
            for(LabelPattern label: parser.labels[0]){
                groupQuery += label.token+",";
            }
            groupQuery = groupQuery.substring(0, groupQuery.length()-1);
            groupQuery += " SET ";
            /*for(LabelPattern label: parser.labels[0]){
                groupQuery += label.token+":"+nextLabel+",";
            }*/
            groupQuery += parser.groupBy.groupLabel+"."+parser.groupBy.operator.getLabel()+" = aggrValue";

            System.out.println(groupQuery);

            db.execute(groupQuery);
            tx.success();
            tx.close();
            groupLabels.put(node.toString(), nextLabel);
        }
    }

}
