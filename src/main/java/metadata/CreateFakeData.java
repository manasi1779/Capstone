package metadata;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CreateFakeData {

    Transaction tx;
    BatchInserter insert;
    File dbpath;

    public static void main(String[] args){
        CreateFakeData cfd = new CreateFakeData();
        cfd.constructGraph();
    }

    public void constructGraph(){
        dbpath = new File("D:/databases/fake_graph.db");
        try {
            insert = BatchInserters.inserter(dbpath);
            Map map = new HashMap();
            map.put("name", "moony");
            insert.createNode(1, map, Label.label("A"), Label.label("G1"));
            map.put("name", "wormtail");
            insert.createNode(2, map, Label.label("A"), Label.label("G1"));
            map.put("name", "prongs");
            insert.createNode(3, map, Label.label("A"), Label.label("G1"));
            map.put("name", "padfoot");
            insert.createNode(4, map, Label.label("A"), Label.label("G1"));
            map.put("name", "harry");
            insert.createNode(5, map, Label.label("B"), Label.label("G1"));
            map.put("name", "hermione");
            insert.createNode(6, map, Label.label("B"), Label.label("G1"));
            map.put("name", "ron");
            insert.createNode(7, map, Label.label("B"), Label.label("G1"));
            map.put("name", "neville");
            insert.createNode(8, map, Label.label("B"), Label.label("G1"));
            insert.createRelationship(1,2, RelationshipType.withName("x"), null);
            insert.createRelationship(2,3, RelationshipType.withName("x"), null);
            insert.createRelationship(3,4, RelationshipType.withName("x"), null);
            insert.createRelationship(4,1, RelationshipType.withName("x"), null);
            insert.createRelationship(4,5, RelationshipType.withName("z"), null);
            insert.createRelationship(5,6, RelationshipType.withName("y"), null);
            insert.createRelationship(6,7, RelationshipType.withName("y"), null);
            insert.createRelationship(7,8, RelationshipType.withName("y"), null);
            insert.createRelationship(8,5, RelationshipType.withName("y"), null);

            map.put("name", "moony");
            insert.createNode(11, map, Label.label("A"), Label.label("G2"));
            map.put("name", "wormtail");
            insert.createNode(12, map, Label.label("A"), Label.label("G2"));
            map.put("name", "prongs");
            insert.createNode(13, map, Label.label("A"), Label.label("G2"));
            map.put("name", "padfoot");
            insert.createNode(14, map, Label.label("A"), Label.label("G2"));
            map.put("name", "harry");
            insert.createNode(15, map, Label.label("B"), Label.label("G2"));
            map.put("name", "hermione");
            insert.createNode(16, map, Label.label("B"), Label.label("G2"));
            map.put("name", "ron");
            insert.createNode(17, map, Label.label("B"), Label.label("G2"));
            map.put("name", "neville");
            insert.createNode(18, map, Label.label("B"), Label.label("G2"));
            insert.createRelationship(11,12, RelationshipType.withName("x"), null);
            insert.createRelationship(12,13, RelationshipType.withName("x"), null);
            insert.createRelationship(13,14, RelationshipType.withName("x"), null);
            insert.createRelationship(14,11, RelationshipType.withName("x"), null);
            insert.createRelationship(14,15, RelationshipType.withName("z"), null);
            insert.createRelationship(15,16, RelationshipType.withName("y"), null);
            insert.createRelationship(16,17, RelationshipType.withName("y"), null);
            insert.createRelationship(17,18, RelationshipType.withName("y"), null);
            insert.createRelationship(18,15, RelationshipType.withName("y"), null);
            insert.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
