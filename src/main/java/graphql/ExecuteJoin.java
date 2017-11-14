package graphql;

import metadata.Edge;
import metadata.LabelPattern;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import parser.Parser;

import java.util.Map;

public class ExecuteJoin {

    NewLabel labelMaker = NewLabel.getInstance();

    @Autowired
    UpdateCypher updateCypher;

    public void join(GraphDatabaseService db, Parser parser){
        Transaction tx = db.beginTx();
        Result rs1 = getJoinResult(db, parser, 0);
        getJoinResult(db, parser, 1);

        while(rs1.hasNext()){
            Map<String, Object> record = rs1.next();
            String createRelationQuery =
                    "Match ("+parser.join.labelToken[0]+":"+parser.labelsMap.get(parser.join.labelToken[0]).name+"{"+
                            parser.join.property[0]+":\""+
                            ((Node)record.get(parser.join.labelToken[0])).getProperty(parser.join.property[0])+
                            "\"}), (" +
                            parser.join.labelToken[1]+":"+parser.labelsMap.get(parser.join.labelToken[1]).name+"{"+
                            parser.join.property[1]+":\""+
                            ((Node)record.get(parser.join.labelToken[0])).getProperty(parser.join.property[0])+
                            "\"}) " +
                            " Set ";

            String nlabel = labelMaker.getNextLabel();
            createRelationQuery += parser.join.labelToken[0]+":"+nlabel+",";
            nlabel = labelMaker.getNextLabel();
            createRelationQuery += parser.join.labelToken[1]+":"+nlabel;
            createRelationQuery += " create ("+parser.join.labelToken[0]+")-[r: join]->("+parser.join.labelToken[1]+")";
            System.out.println(createRelationQuery);
            db.execute(createRelationQuery);
        }
        tx.success();
        tx.close();
    }

    public Result getJoinResult(GraphDatabaseService db, Parser parser, int index){
        String cypherQuery1 = "Match ";

        for(Edge edge: parser.edges){
            if(parser.labels[index].contains(parser.labelsMap.get(edge.from.token)))
                cypherQuery1 += edge.plainString()+",";
        }
        cypherQuery1 = cypherQuery1.substring(0, cypherQuery1.length()-1);
        cypherQuery1 += " where ";

        for(LabelPattern node: parser.labels[index]) {
            cypherQuery1 += node.token + ":" + parser.labelsMap.get(node.token).name + " AND " + node.token + ":" + parser.from[index] + " AND ";
        }
        cypherQuery1 = cypherQuery1.substring(0, cypherQuery1.length()-4);
        cypherQuery1 = updateCypher.addMatchingWhere(cypherQuery1, parser, parser.labels[index]);

        cypherQuery1 += " Set ";
        for(LabelPattern label: parser.labels[index]){
            String nextlabel = labelMaker.getNextLabel();
            cypherQuery1 += label.token+":"+nextlabel+",";
            LabelPattern labelPattern = parser.labelsMap.get(label.token);
            labelPattern.name = nextlabel;
        }
        cypherQuery1= cypherQuery1.substring(0, cypherQuery1.length()-1);
        cypherQuery1 += " Return "+parser.join.labelToken[index];
        System.out.println(cypherQuery1);
        Result rs1 = db.execute(cypherQuery1);
        return rs1;
    }

}
