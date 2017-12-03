package graphql;

import metadata.*;
import org.springframework.stereotype.Component;
import parser.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.util.*;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.beans.factory.annotation.Autowired;

@Component
public class QueryCypher {

    private GraphDatabaseService db;
    NewLabel newLabel;

    @Autowired
    ExecuteGroupBy executeGroupBy;
    @Autowired
    ExecuteJoin executeJoin;
    @Autowired
    ExecuteMatch executeMatch;
    @Autowired
    ExecutePredicate executePredicate;
    @Autowired
    UpdateCypher updateCypher;

    public static void main(String[] args){
        ApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");
        QueryCypher qc = context.getBean(QueryCypher.class);
        File dataFolder = new File("D:\\databases\\connected_graph.db");
        qc.db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dataFolder).setConfig(GraphDatabaseSettings.pagecache_memory, "256M" ).setConfig(GraphDatabaseSettings.string_block_size, "60" ).setConfig(GraphDatabaseSettings.array_block_size, "300" ).newGraphDatabase();
        qc.newLabel = NewLabel.getInstance();
        System.out.println("Enter query");
        Parser compositeParser = qc.getQueryChain();
        qc.createAndRunDecomposedQuery(compositeParser);
        qc.db.shutdown();
    }

    /**
     * Update the labels of the query graph as query is executed and
     * @param parser
     */
    public void createAndRunDecomposedQuery(Parser parser){
        if(parser.join != null){
            executeJoin.join(db, parser);
        }
        else {
            //warm up and then perform comparisons
            createAndRunCompleteCypherQuery(parser);
        /*    applyPatternThenPredicate(parser);
            if(parser.groupBy != null){
                executeGroupBy.groupBy(db, parser);
            }
            newLabel.tearDown(db);
            //before executing new query, ned to reset the labels map
        */
            applyPredicateThenPattern(parser);
            if(parser.groupBy != null){
                executeGroupBy.groupBy(db, parser);
            }
        //    newLabel.tearDown(db);
        }
        newLabel.tearDown(db);
    }

    public void createAndRunCompleteCypherQuery(Parser parser){
        String completeCypherQuery = updateCypher.addMatch(parser.edges);
        if(parser.wheres.size() != 0){
            completeCypherQuery = updateCypher.addWheres(completeCypherQuery, parser.wheres);
            completeCypherQuery = updateCypher.addIntermediateReturn(completeCypherQuery, parser.project, parser.wheres);
        }
        else{
            completeCypherQuery = updateCypher.addReturn(completeCypherQuery, parser.labels[0]);
        }
        System.out.println(completeCypherQuery);
        executeCypherQuery(completeCypherQuery);
    }

    /**
     * This method finds the pattern by constructing cypher query that specifies edges
     * @return
     */
    public void applyPatternThenPredicate(Parser parser){
        Transaction tx = db.beginTx();
        Long t1 = System.currentTimeMillis();
        executeMatch.constructMatchSetReturn(db,parser);
        tx.success();
        tx.close();
        executePredicate.applyPredicateQuery(db, parser);
        Long t2 = System.currentTimeMillis();
        System.out.println("Optimized query took "+ (t2 - t1));
    }

    public void applyPredicateThenPattern(Parser parser){
        Long t1 = System.currentTimeMillis();
        executePredicate.applyPredicateQuery(db, parser);
        Transaction tx = db.beginTx();
        executeMatch.constructMatchSetReturn(db,parser);
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
        ArrayList<Object> projects = new ArrayList<>();
        ArrayList<Edge> edges = new ArrayList<>();
        ArrayList<Group> groups = new ArrayList<>();
        AggrOperator operator = null;
        Join join = null;
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
            parser.groupBy.stream().forEach(
                o -> {
                    if (! groups.contains((o))){
                        groups.add(o);
                    }
                }
            );
            edges.addAll(parser.edges);
            labelMap.putAll(parser.labelsMap);
            if(parser.join != null)
                join = parser.join;
            if(parser.operator != null){
                operator = parser.operator;
            }
        }
        Parser parser = new Parser(System.in);
        parser.project = projects;
        parser.wheres = wheres;
        parser.labels[0] = labels;
        parser.labelsMap = labelMap;
        parser.edges = edges;
        parser.join = join;
        parser.groupBy = groups;
        parser.operator = operator;
        return parser;
    }


    private void executeCypherQuery(String cypherQuery) {
        Long t1 = System.currentTimeMillis();
        Transaction tx = db.beginTx();
        Result rs = db.execute(cypherQuery);
        Long t2 = System.currentTimeMillis();
        System.out.println("Run cypher query in "+ (t2 - t1));
        while(rs.hasNext()){
            Map<String, Object> res = rs.next();
            printSearchSpace(res);
        }
        tx.success();
        tx.close();
    }

    private void printSearchSpace(Map<String, Object> result){
        for(String key: result.keySet()){
            System.out.print(key+":"+result.get(key)+" ");
        }
        System.out.println();
    }

}