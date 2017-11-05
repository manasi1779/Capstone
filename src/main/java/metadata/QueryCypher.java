package metadata;

import graphql.*;
import parser.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.util.*;

public class QueryCypher {

    private GraphDatabaseService db;

    public static void main(String[] args){
        QueryCypher qc = new QueryCypher();
        File dataFolder = new File("D:\\databases\\fake_graph.db");
        qc.db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dataFolder).setConfig(GraphDatabaseSettings.pagecache_memory, "256M" ).setConfig(GraphDatabaseSettings.string_block_size, "60" ).setConfig(GraphDatabaseSettings.array_block_size, "300" ).newGraphDatabase();
        Scanner s = new Scanner(System.in);
        display();
        int option = s.nextInt();
        switch (option){
            case 1:{
                qc.runQuery();
                break;
            }
            case 2:{
                Parser compositeParser = qc.getQueryChain();
                qc.createAndRunDecomposedQuery(compositeParser);
            //    qc.createAndRunCompleteCypherQuery(compositeParser);
                break;
            }
            default:{
                System.out.println("Wrong option");
            }
        }
    }

    public static void display(){
        System.out.println("Choose mode");
        System.out.println("High level query language for Graph DB");
        System.out.println("1. Single query");
        System.out.println("2. Multiple query");
        System.out.println("Enter option 1 or 2");
    }

    public void runQuery(){
        Parser parser = new Parser(System.in);
        try {
            parser.Start();
            // createAndRunCompleteCypherQuery(parser);
            //TODO: clear cache
            // ((GraphDatabaseAPI)qc.db).getDependencyResolver().resolveDependency( Cache.class ).clear();
            createAndRunDecomposedQuery(parser);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void createAndRunCompleteCypherQuery(Parser parser){
        String completeCypherQuery = addMatch(parser.edges);
        completeCypherQuery = addWheres(completeCypherQuery, parser.wheres);
        completeCypherQuery = addReturn(completeCypherQuery, parser.project);
        System.out.println(completeCypherQuery);
        executeCypherQuery(completeCypherQuery);
    }

    /**
     * Update the labels of the query graph as query is executed and
     * @param parser
     */
    public void createAndRunDecomposedQuery(Parser parser){
        if(parser.join != null){
            join(parser);
            tearDown();
        }
        else {
            applyPatternThenPredicate(parser);
            tearDown();
            applyPredicateThenPattern(parser);
            tearDown();
        }
    }

    public String constructMatchSetReturn(Parser parser){
        String cypherQuery = addMatch(parser.edges);
        // put in labelsMap new label and set it to result as well
        cypherQuery += "Set ";
        for(LabelPattern label: parser.labels[0]){
            String nextlabel = getNextLabel();
            cypherQuery += label.token+":"+nextlabel+",";
            LabelPattern labelPattern = parser.labelsMap.get(label.token);
            labelPattern.name = nextlabel;
            newLabels.add(nextlabel);
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1);
        cypherQuery = addIntermediateReturn(cypherQuery, parser.project, parser.wheres);
        System.out.println(cypherQuery);
        return cypherQuery;
    }

	/*
	    Find pattern first then filter as per predicate
	    or apply predicate first then find pattern
	    Find instances satisfying pattern and then apply the predicate in memory
	    Apply predicate using cypher and then make join in memory
	    predicate should be applied to attribute f node
    	Get Nodes and attributes first. Store filtering criteria differently.
	*/
	public String addMatchingWhere(String query, Parser parser, ArrayList<LabelPattern> labels){
        for(ComplexWhere complexWhere: parser.wheres){
            for(Where where: complexWhere.wheres){
                if(labels.contains(parser.labelsMap.get(where.labelToken)))
                    query += " AND " + where;
            }
        }
        return query;
    }

    /**
     * This method finds the pattern by constructing cypher query that specifies edges
     * @return
     */
    public void applyPatternThenPredicate(Parser parser){
        Transaction tx = db.beginTx();
        Long t1 = System.currentTimeMillis();
        String cypherQuery = constructMatchSetReturn(parser);
        db.execute(cypherQuery);
        tx.success();
        tx.close();
        applyPredicateQuery(parser);
        Long t2 = System.currentTimeMillis();
        System.out.println("Optimized query took "+ (t2 - t1));
    }

    public void applyPredicateThenPattern(Parser parser){
        Long t1 = System.currentTimeMillis();
        applyPredicateQuery(parser);
        String cypherQuery = constructMatchSetReturn(parser);
        Transaction tx = db.beginTx();
        db.execute(cypherQuery);
        Long t2 = System.currentTimeMillis();
        System.out.println("Optimized query took "+ (t2 - t1));
        tx.success();
        tx.close();
    }

    public Parser getQueryChain(){
        ArrayList<Parser> parsers = new ArrayList<>();
        while(true){
            try {
                Parser parser = new Parser(System.in);
                parser.Start();
                System.out.println(parser.labelsMap.size());
                if(parser.labelsMap.size() == 0)
                    break;
                parsers.add(parser);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return collectQueryChain(parsers);
    }

    public Parser collectQueryChain(ArrayList<Parser> parsers){
        ArrayList<ComplexWhere> wheres = new ArrayList<>();
        ArrayList<LabelPattern> labels = new ArrayList<>();
        ArrayList<String> projects = new ArrayList<>();
        ArrayList<Edge> edges = new ArrayList<>();
        HashMap labelMap = new HashMap();
        for(Parser parser: parsers){
            wheres.addAll(parser.wheres);
            parser.labels[0].stream().forEach(
                o -> {
                    System.out.println(o);
                    if(!labels.contains(o)) {
                        labels.add(o);
                    }
                    else{
                        LabelPattern label1 = labels.get(labels.indexOf(o));
                        o.properties.stream().forEach(
                            p ->{
                                if(!label1.properties.contains(p)) {
                                    label1.properties.add(p);
                                }
                            }
                        );
                    }
                }
            );
            parser.project.stream().forEach(
                o -> {
                    System.out.println(o);
                    if(!projects.contains(o)) {
                        projects.add(o);
                    }
                }
            );
            edges.addAll(parser.edges);
            labelMap.putAll(parser.labelsMap);
        }
        Parser parser = new Parser(System.in);
        parser.project = projects;
        parser.wheres = wheres;
        parser.labels[0] = labels;
        parser.labelsMap = labelMap;
        parser.edges = edges;
        return parser;
    }

    public String addMatch(ArrayList<Edge> edges){
        String cypherQuery = "Match ";
        for(Edge edge: edges){
            cypherQuery += edge+",";
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1);
        return cypherQuery;
    }

    public String addWheres(String cypherQuery, ArrayList<ComplexWhere> wheres){
        cypherQuery += " where";
        for(ComplexWhere complexWhere : wheres){
            if (complexWhere instanceof WhereAnd){
                for (Where where : complexWhere.wheres) {
                    cypherQuery += " "+where+" AND";
                }
                cypherQuery = cypherQuery.substring(0, cypherQuery.length()-3);
            } else {
                for (Where where : complexWhere.wheres) {
                    cypherQuery += " " + where + " OR";
                }
                cypherQuery = cypherQuery.substring(0, cypherQuery.length()-2);
            }
            cypherQuery += " AND";
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-3);
        return cypherQuery;
    }

    public String addReturn(String cypherQuery, ArrayList<String> labels){
        cypherQuery += " return ";
        for(String label: labels){
            cypherQuery += label+",";
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1);
        return cypherQuery;
    }

    String addIntermediateReturn(String cypherQuery, ArrayList<String> required, ArrayList<ComplexWhere> wheres){
        cypherQuery += " return ";
        for(String label: required){
            cypherQuery += label+",";
        }
        HashSet<String> addedTokens = new HashSet<>();
        for(ComplexWhere complexWhere: wheres){
            for(Where where: complexWhere.wheres){
                if(!required.contains(where.labelToken) && !addedTokens.contains(where.labelToken)){
                    cypherQuery += where.labelToken+",";
                    addedTokens.add(where.labelToken);
                }
            }
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1);
        return cypherQuery;
    }

    private List<Map> executeCypherQuery(String cypherQuery) {
        Long t1 = System.currentTimeMillis();
        Transaction tx = db.beginTx();
        Result rs = db.execute(cypherQuery);
        Long t2 = System.currentTimeMillis();
        List<Map> result = new ArrayList<>();
        System.out.println("Run cypher query in "+ (t2 - t1));
        while(rs.hasNext()){
            Map<String, Object> res = rs.next();
            result.add(res);
            printSearchSpace(res);
        }
        tx.close();
        return result;
    }

    private void printSearchSpace(Map<String, Object> result){
        for(String key: result.keySet()){
            System.out.print(key+":"+result.get(key)+" ");
        }
        System.out.println();
    }


    /**
     * Apply selection and relabel nodes with new label
     * Use new labels for next operation.     *
     * @param parser
     */
    public void applyPredicateQuery(Parser parser){
        Optimizer optimizer = new Optimizer();
        ArrayList<ComplexWhere> whereOrder = optimizer.getSortedWheres(db, parser);
        HashMap<String, ArrayList<Where>> labelWheresMap = new HashMap();
        for(ComplexWhere complexWhere: whereOrder){
            for(Where where: complexWhere.wheres){
                if(!labelWheresMap.containsKey(where.labelToken))
                    labelWheresMap.put(where.labelToken, new ArrayList());
                labelWheresMap.get(where.labelToken).add(where);
            }
        }
        String newLabel = getNextLabel();
        newLabels.add(newLabel);
        for(String key: labelWheresMap.keySet()){
            String cypherQuery = "Match ("+ key+":"+parser.labelsMap.get(key).name+"{";
            for(Where where: labelWheresMap.get(key)){
                cypherQuery += where.property+":\""+where.value+"\",";
            }
            cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1)+"} "+")";
            cypherQuery += "set "+key+":"+newLabel;
            LabelPattern changeLabel = parser.labelsMap.get(key);
            changeLabel.name = newLabel;
            System.out.println(cypherQuery);
            Transaction tx = db.beginTx();
            db.execute(cypherQuery);
            tx.close();
        }
    }

    public void tearDown(){
        for(String key: newLabels){
            String cypherQuery = "Match ( x :"+key+") Remove x :"+key;
            System.out.println(cypherQuery);
            Transaction tx = db.beginTx();
            db.execute(cypherQuery);
            tx.success();
            tx.close();
        }
    }

    int labelCount = 1;
    ArrayList<String> newLabels = new ArrayList<>();

    public String getNextLabel(){
        return "Graph"+labelCount++;
    }

    public void join(Parser parser){
        Transaction tx = db.beginTx();
        Result rs1 = getJoinResult(parser, 0);
        Result rs2 = getJoinResult(parser, 1);
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
            String nlabel =     getNextLabel();
            newLabels.add(nlabel);
            createRelationQuery += parser.join.labelToken[0]+":"+nlabel+",";
            nlabel = getNextLabel();
            newLabels.add(nlabel);
            createRelationQuery += parser.join.labelToken[1]+":"+nlabel;
            createRelationQuery += " create ("+parser.join.labelToken[0]+")-[r: join]->("+parser.join.labelToken[1]+")";
            System.out.println(createRelationQuery);
            db.execute(createRelationQuery);
        }

    /*    while(rs2.hasNext()){
            Map<String, Object> record = rs2.next();
            String createRelationQuery =
                    "Match ("+parser.join.labelToken[0]+":"+parser.labelsMap.get(parser.join.labelToken[0]).name+"{"+
                            parser.join.property[0]+":\""+
                            ((Node)record.get(parser.join.labelToken[1])).getProperty(parser.join.property[1])+
                            "\"}), (" +
                            parser.join.labelToken[1]+":"+parser.labelsMap.get(parser.join.labelToken[1]).name+"{"+
                            parser.join.property[1]+":\""+
                            ((Node)record.get(parser.join.labelToken[1])).getProperty(parser.join.property[1])+
                            "\"}) " +
                            " Set ";
            String nlabel =     getNextLabel();
            newLabels.add(nlabel);
            createRelationQuery += parser.join.labelToken[0]+":"+nlabel+",";
            nlabel = getNextLabel();
            newLabels.add(nlabel);
            createRelationQuery += parser.join.labelToken[1]+":"+nlabel;
            createRelationQuery += " create ("+parser.join.labelToken[0]+")-[r: join]->("+parser.join.labelToken[1]+")";
            System.out.println(createRelationQuery);
            db.execute(createRelationQuery);
        }
    */    tx.success();
        tx.close();
    }

    public Result getJoinResult(Parser parser, int index){
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
        cypherQuery1 = addMatchingWhere(cypherQuery1, parser, parser.labels[index]);

        cypherQuery1 += " Set ";
        for(LabelPattern label: parser.labels[index]){
            String nextlabel = getNextLabel();
            cypherQuery1 += label.token+":"+nextlabel+",";
            LabelPattern labelPattern = parser.labelsMap.get(label.token);
            labelPattern.name = nextlabel;
            newLabels.add(nextlabel);
        }
        cypherQuery1= cypherQuery1.substring(0, cypherQuery1.length()-1);
        cypherQuery1 += " Return "+parser.join.labelToken[index];
        System.out.println(cypherQuery1);
        Result rs1 = db.execute(cypherQuery1);
        return rs1;
    }
}