package graphql;

import metadata.*;
import parser.Parser;

import java.util.ArrayList;
import java.util.HashSet;

public class UpdateCypher {

    public String addMatch(ArrayList<Edge> edges){
        String cypherQuery = "Match ";
        for(Edge edge: edges){
            cypherQuery += edge+",";
        }
        cypherQuery = cypherQuery.substring(0, cypherQuery.length()-1);
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
    public static String addMatchingWhere(String query, Parser parser, ArrayList<LabelPattern> labels){
        for(ComplexWhere complexWhere: parser.wheres){
            for(Where where: complexWhere.wheres){
                if(labels.contains(parser.labelsMap.get(where.labelToken)))
                    query += " AND " + where;
            }
        }
        return query;
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
                for (Where where : complexWhere.wheres){
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

    public String addIntermediateReturn(String cypherQuery, ArrayList<Object> required, ArrayList<ComplexWhere> wheres){
        cypherQuery += " return ";
        for(Object label: required){
            if(label instanceof String)
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


}
