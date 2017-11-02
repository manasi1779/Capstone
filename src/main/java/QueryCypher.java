import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.util.*;

public class QueryCypher {

    private GraphDatabaseService db;

    public static void main(String[] args){
        QueryCypher qc = new QueryCypher();
        File dataFolder = new File("D:\\databases\\small_graph.db");
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
            createAndRunCompleteCypherQuery(parser);
            //TODO: clear cache
            //((GraphDatabaseAPI)qc.db).getDependencyResolver().resolveDependency( Cache.class ).clear();
        //    createAndRunDecomposedQuery(parser);
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
        String cypherQuery = addMatch(parser.edges);
        // put in labelsMap new label and set it to result as well
        cypherQuery += "Set ";
        for(LabelPattern label: parser.labels1){
            String nextlabel = getNextLabel();
            cypherQuery += label.token+":"+nextlabel+",";
            LabelPattern labelPattern = parser.labelsMap.get(label.token);
            labelPattern.name = nextlabel;
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1);
        cypherQuery = addIntermediateReturn(cypherQuery, parser.project, parser.wheres);
        System.out.println(cypherQuery);
        if(parser.join != null)
            makeJoinWithoutWhere(parser);
        else {
            findPattern(cypherQuery, parser);
        }
    }

    public void makeJoinWithoutWhere(Parser parser){
        String completeQuery = "Match ";
        String cypherQuery1 = "Match ";
        for(Edge edge: parser.edges){
            if(parser.labels1.contains(parser.labelsMap.get(edge.from.token)))
            cypherQuery1 += edge.plainString()+",";
            completeQuery += edge.plainString()+",";
        }
        cypherQuery1 = cypherQuery1.substring(0, cypherQuery1.length()-1);
        cypherQuery1 += " where ";

        completeQuery = completeQuery.substring(0, completeQuery.length()-1);
        completeQuery += " where ";

        for(LabelPattern node: parser.labels1) {
            cypherQuery1 += node.token + ":" + parser.labelsMap.get(node.token).name + " AND " + node.token + ":" + parser.from1 + " AND ";
            completeQuery += node.token + ":" + parser.labelsMap.get(node.token).name + " AND " + node.token + ":" + parser.from1 + " AND ";
        }
        cypherQuery1 = cypherQuery1.substring(0, cypherQuery1.length()-4);

        cypherQuery1 = addMatchingWhere(cypherQuery1, parser, parser.labels1);
        cypherQuery1 = addReturn(cypherQuery1, parser.project);
        System.out.println(cypherQuery1);

        String cypherQuery2 = "Match ";
        for(Edge edge: parser.edges){
            if(parser.labels2.contains(parser.labelsMap.get(edge.from.token)))
                cypherQuery2 += edge.plainString()+",";
        }
        cypherQuery2 = cypherQuery2.substring(0, cypherQuery2.length()-1);
        cypherQuery2 += " where ";
        for(LabelPattern node: parser.labels2) {
            cypherQuery2 += node.token + ":" + parser.labelsMap.get(node.token).name + " AND " + node.token + ":" + parser.from2 + " AND ";
            completeQuery += node.token + ":" + parser.labelsMap.get(node.token).name + " AND " + node.token + ":" + parser.from2 + " AND ";
        }
        completeQuery = completeQuery.substring(0, completeQuery.length()-4);
        cypherQuery2 = cypherQuery2.substring(0, cypherQuery2.length()-4);

        cypherQuery2 = addMatchingWhere(cypherQuery2, parser, parser.labels2);
        cypherQuery2 = addReturn(cypherQuery2, parser.project);
        completeQuery += " AND ";
        completeQuery = addWheres(completeQuery, parser.wheres);
        completeQuery = completeQuery.substring(0, completeQuery.lastIndexOf("where")) + completeQuery.substring(completeQuery.lastIndexOf("where") + 5);
        completeQuery = addReturn(completeQuery, parser.project);
        completeQuery += ",";
        completeQuery = addReturn(completeQuery, parser.project);
        completeQuery = completeQuery.substring(0, completeQuery.lastIndexOf("return")) + completeQuery.substring(completeQuery.lastIndexOf("return") + 6);
        System.out.println("Complete query "+completeQuery);
        System.out.println(cypherQuery2);
        executeCypherQuery(completeQuery);
    }

    /**
     * Writing cypher query
     * MATCH (m:movie)-[r: directed_by]-
     * (d:director{first_name:'Steven',last_name:'Spielberg'}) match
     * (m)-[:movie_genre]-(g:genre{genre:'Drama'}) RETURN m
     */

	/*
		Select with criteria, with labels
		We should go for labels
		Example working with two graph representations
	*/

	/*
	    Match -> Selection -> Projection
	    Projection -> Match -> Selection
	    Selection -> Projection -> Match
	 */

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
    public void findPattern(String cypherQuery, Parser parser){
        Transaction tx = db.beginTx();
        Long t1 = System.currentTimeMillis();
        db.execute(cypherQuery);
        tx.success();
        tx.close();
        applyPredicateQuery(parser);

        Long t2 = System.currentTimeMillis();
        System.out.println("Optimized query took "+ (t2 - t1));
    }

    public void applyProjection(List<Map> results, ArrayList<String> required){
        for(Map result: results){
            result.keySet().retainAll(required);
        }
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
            parser.labels1.stream().forEach(
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
        parser.labels1 = labels;
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
            if (complexWhere instanceof WhereAnd) {
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
        ArrayList<ComplexWhere> whereOrder = optimizer.getPredicateOrder(db, parser);
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
            tx.close();
        }
    }

    int labelCount = 1;
    ArrayList<String> newLabels = new ArrayList<>();

    public String getNextLabel(){
        return "G"+labelCount++;
    }


    /**
     * Method for composition. Different operators should be able to change sequence
     * within themselves. Is there only one valid sequence of operators?
     * No, operators are interchangeable in the sequence
     * Current goal: write the result of operator to new database
     * Apply next operators on top of that database
     */
    public void performJoin(Join join, List<Map> result1, List<Map> result2, HashMap<String, LabelPattern> labelsMap){
        Transaction tx = db.beginTx();
        for(Map res: result1){
            for(Map res2: result2){
                Object value1 = null, value2 = null;
                if(res.containsKey(join.labelToken1))
                    value1 = ((Node)res.get(join.labelToken1)).getProperty(join.property1);
                if(res2.containsKey(join.labelToken2))
                    value2 = ((Node)res2.get(join.labelToken2)).getProperty(join.property2);
                if(value1 != null && value2 != null){
                    if(join.performOperation(value1,value2)){
                        ((Node) res.get(join.labelToken1)).createRelationshipTo((Node)res2.get(join.labelToken2), RelationshipType.withName("join on "+join));
                    }
                }
                value1 = null; value2 = null;
                if(res.containsKey(join.labelToken2))
                    value1 = ((Node)res.get(join.labelToken2)).getProperty(join.property2);
                if(res2.containsKey(join.labelToken1))
                    value2 = ((Node)res2.get(join.labelToken1)).getProperty(join.property1);
                if(value1 != null && value2 != null && join.performOperation(value1, value2))
                    ((Node) res.get(join.labelToken1)).createRelationshipTo((Node)res2.get(join.labelToken2), RelationshipType.withName("join on "+join));         }
        }
        tx.success();
        tx.close();
    }
}