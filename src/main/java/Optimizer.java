import org.neo4j.graphdb.*;

import java.util.*;
import java.util.stream.Collectors;

public class Optimizer {

    String queryOrder;

    /**
     * Since the where clause having that label as attribute
     * which has minimum distinct values is most selective, order where clauses according to that
     * @param parser
     * @param db
     */
    public static ArrayList<ComplexWhere> getSortedWheres(GraphDatabaseService db, Parser parser){
        HashMap<String, Long> counts = new HashMap<>();
        Transaction tx = db.beginTx();

        for(ComplexWhere complexWhere: parser.wheres){
            for (Where where: complexWhere.wheres){
                String distinctQuery = "MATCH (node:"+parser.labelsMap.get(where.labelToken).name+") return collect(distinct node."+where.property+")";
                Result result = db.execute(distinctQuery);
                counts.put(parser.labelsMap.get(where.labelToken).name, result.stream().count());
                tx.success();
            }
        }
        tx.close();
        ArrayList<ComplexWhere> sortedComplexWheres = new ArrayList<>();
        parser.wheres.stream().sorted(new Comparator<ComplexWhere>() {
            @Override
            public int compare(ComplexWhere o1, ComplexWhere o2) {
                if(o1 instanceof WhereAnd && o2 instanceof WhereAnd)
                    return o1.wheres.size() - o2.wheres.size();
                else if(o1 instanceof WhereOr && o2 instanceof WhereAnd)
                    return -1;
                else if(o1 instanceof WhereOr && o2 instanceof WhereOr)
                    return o1.wheres.size() - o2.wheres.size();
                else
                    return 1;
            }
        }).forEach(
            o -> {
                ComplexWhere complexWhere;
                if(o instanceof WhereAnd)
                    complexWhere = new WhereAnd();
                else
                    complexWhere = new WhereOr();
                List<Where> sortedWheres =
                o.wheres.stream().sorted(new Comparator<Where>() {
                    @Override
                    public int compare(Where o1, Where o2) {
                        return (int) (counts.get(parser.labelsMap.get(o1.labelToken).name) - counts.get(parser.labelsMap.get(o2.labelToken).name));
                    }
                }).collect(Collectors.toList());
                complexWhere.wheres = sortedWheres;
                sortedComplexWheres.add(complexWhere);
            }
        );
        return sortedComplexWheres;
    }

    /**
     * This method finds the optimal predicate order by sorting the Complex where clauses
     * by size of individual where clauses in it and sorting the individual where clauses by
     * number of nodes in target graph that the labels in where clause cover.
     * Hypothesis is since predicate covers more number of nodes it will filter more
     * So priority is given to predicate having label with more number of nodes.
     * @return
     */
    public ArrayList<ComplexWhere> getPredicateOrder(GraphDatabaseService db, Parser parser){
        ArrayList<ComplexWhere> whereOrder = new ArrayList<>();
        HashMap<ComplexWhere, HashMap<Where, Long>> span = new HashMap<>();
        Transaction tx = db.beginTx();
        for(ComplexWhere complexWhere: parser.wheres){
            HashMap<Where, Long> counts = new HashMap<>();
            for(Where where: complexWhere.wheres){
                ResourceIterator<Node> nodes = db.findNodes(Label.label(parser.labelsMap.get(where.labelToken).name));
                long count = nodes.stream().count();
                counts.put(where, count);
            }
            span.put(complexWhere, counts);
        }
        tx.success();
        tx.close();
        span.entrySet().stream().sorted(Map.Entry.<ComplexWhere, HashMap<Where, Long>>comparingByValue(new Comparator<HashMap>(){
            @Override
            public int compare(HashMap o1, HashMap o2) {
                return o1.size() - o2.size();
            }
        })).forEachOrdered(o->{
            HashMap<Where, Long> whereMap = o.getValue();
            ComplexWhere complexWhereNew;
            if(o.getKey() instanceof WhereAnd)
                complexWhereNew = new WhereAnd();
            else
                complexWhereNew = new WhereOr();
            whereMap.entrySet().stream().sorted(Map.Entry.<Where, Long>comparingByValue()).forEachOrdered(x -> {
                complexWhereNew.wheres.add(x.getKey());
            });
            whereOrder.add(complexWhereNew);
        });
        return whereOrder;
    }

    public void getQueryOrder(){

    }


}
