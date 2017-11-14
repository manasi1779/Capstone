package graphql;

import metadata.ComplexWhere;
import metadata.LabelPattern;
import metadata.Where;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import parser.Parser;

import java.util.ArrayList;
import java.util.HashMap;

public class ExecutePredicate {

    public void applyPredicateQuery(GraphDatabaseService db, Parser parser){
        Optimizer optimizer = new Optimizer();
        ArrayList<ComplexWhere> whereOrder = optimizer.getPredicateOrder(db, parser);
        HashMap<String, ArrayList<Where>> labelWheresMap = new HashMap();
        for(ComplexWhere complexWhere: whereOrder){
            for(Where where: complexWhere.wheres){
                if(!labelWheresMap.containsKey(where.labelToken))
                    labelWheresMap.put(where.labelToken, new ArrayList());
                labelWheresMap.get(where.labelToken).add(where);
            }
        }
        NewLabel labelMaker = NewLabel.getInstance();
        String newLabel = labelMaker.getNextLabel();
        for(String key: labelWheresMap.keySet()){
            String cypherQuery = "Match ("+ key+":"+parser.labelsMap.get(key).name+"{";
            for(Where where: labelWheresMap.get(key)){
                cypherQuery += where.property+":\""+where.value+"\",";
            }
            cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1)+"} "+")";
            cypherQuery += " set "+key+":"+newLabel;
            LabelPattern changeLabel = parser.labelsMap.get(key);
            changeLabel.name = newLabel;
            System.out.println(cypherQuery);
            Transaction tx = db.beginTx();
            db.execute(cypherQuery);
            tx.success();
            tx.close();
        }
    }
}
