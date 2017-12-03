package graphql;

import metadata.Group;
import metadata.LabelPattern;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import parser.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ExecuteGroupBy {

    @Autowired
    UpdateCypher updateCypher;

    HashMap<String, String> groupLabels = new HashMap<>();

    //Single group by
    public void groupBy(GraphDatabaseService db, Parser parser, Group group){

        // Add new label to each node with value of group clause
        String distinctQuery = "MATCH (node:"+parser.labelsMap.get(group.groupLabel).name+") return distinct node."+group.property;
        Transaction tx = db.beginTx();
        Result result = db.execute(distinctQuery);
        String query = updateCypher.addMatch(parser.edges);
        NewLabel labelMaker = NewLabel.getInstance();
        while (result.hasNext()){
            String nextLabel = labelMaker.getNextLabel();
            Map<String, Object> res = result.next();
            String node = res.get("node."+group.property).toString();
            if(node.contains("\""))
                continue;
            System.out.println(node);
            String groupQuery = query;
            //TODO multiple group by, concat the group bys in order by executing nested while
            groupQuery += " WHERE "+group.groupLabel+"."+
                    group.property+"= \""+node+"\"";
            groupQuery += " WITH "+parser.operator+" AS aggrValue, ";
            for(LabelPattern label: parser.labels[0]){
                groupQuery += label.token+",";
            }
            groupQuery = groupQuery.substring(0, groupQuery.length()-1);
            groupQuery += " SET ";
            /*for(LabelPattern label: parser.labels[0]){
                groupQuery += label.token+":"+nextLabel+",";
            }*/
            groupQuery += group.groupLabel+"."+parser.operator.getLabel()+" = aggrValue";

            System.out.println(groupQuery);

            db.execute(groupQuery);
            tx.success();
            tx.close();
            groupLabels.put(node.toString(), nextLabel);
        }
    }

    //Multiple group by criteria and single aggregation operator
    public void groupBy(GraphDatabaseService db, Parser parser){
        //get distinct values for all attributes in the group by list
        //add relationship to the match clause if node is in group by list
        calcDistinctValues(db, parser);
        String query = updateCypher.addMatch(parser.edges) + " WHERE ";
        addAttribute(db, parser.groupBy, 0, query, parser, "");
    }

    ArrayList<ArrayList> distinctValues;

    public void calcDistinctValues(GraphDatabaseService db, Parser parser){
        distinctValues = new ArrayList();
        for(Group group: parser.groupBy){
            System.out.println(group);
            ArrayList<String> values = new ArrayList();
            String distinctQuery = "MATCH (node:"+parser.labelsMap.get(group.groupLabel).name+") return distinct node."+group.property;
            Transaction tx = db.beginTx();
            Result result = db.execute(distinctQuery);
            while (result.hasNext()){
                Map<String, Object> res = result.next();
                String node = res.get("node."+group.property).toString();
                if(node.contains("\""))
                    continue;
                values.add(node);
            }
            tx.close();
            distinctValues.add(values);
        }
    }

    public void addAttribute(GraphDatabaseService db, ArrayList<Group> attributes, int index, String query, Parser parser, String attributeLabel){
        Group group = attributes.get(index);
        ArrayList<String> values = distinctValues.get(index);
        for(int i = 0; i < values.size(); i++) {
            String newQuery = query + group.groupLabel + "." +
                    group.property + "= \"" + values.get(i) + "\" AND ";
            String newLabel = attributeLabel + values.get(i).
                    replace(" ", "_").
                    replace("(", "").
                    replace("-", "_").
                    replace(")", "") + "_" ;
            if(index == attributes.size() - 1){
                newQuery = newQuery.substring(0, newQuery.length()-4);
                newQuery += " WITH " + parser.operator + " AS aggrValue, ";
                //Is this necessary or just add the required alias
                for (LabelPattern label : parser.labels[0]) {
                    newQuery += label.token + ",";
                }
                newQuery = newQuery.substring(0, newQuery.length() - 1);
                newQuery += " SET ";
                newQuery += group.groupLabel + "." +newLabel+parser.operator.getLabel() + " = aggrValue";
                System.out.println(newQuery);
                Transaction tx = db.beginTx();
                db.execute(newQuery);
                tx.success();
                tx.close();
            }
            else{
                addAttribute(db, attributes, index+1, newQuery, parser, newLabel);
            }
        }
    }

}
