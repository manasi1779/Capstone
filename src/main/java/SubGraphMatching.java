import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public class SubGraphMatching {

	private GraphDatabaseService db;
	private ArrayList<Long> visitedGreedy;
	private String dbPath;
	private HashMap<Long, String> queryNodes;
	private HashMap<Long, ArrayList<String>> queryEdges;
	private HashMap<Long, HashSet<Long>> queryEdgeNodes;
	private HashMap<Long, HashSet<Long>> searchSpace;
	private HashMap<Long, HashSet<Long>> defaultSearchSpace;
	private ArrayList<Long> queryNodeList;
	private HashMap<Long, Long> newSearchSpace;
	private ArrayList<HashMap<Long, Long>> resultSet;
	private double gamma = 0.5;
	private static HashMap<String, String> querySet = new HashMap<>();
	
	public static void main(String[] args) {
		SubGraphMatching profileMatching = new SubGraphMatching();
	}
	
	private SubGraphMatching(){
		dbPath = "D:/neo4j-enterprise-3.1.1-windows/neo4j-enterprise-3.1.1/data/databases/graph.db/";
	}

	private void getQueryGraph(File fileName){
		try {
			Scanner	s = new Scanner(fileName);			
			String nodesNumber = s.nextLine();
			int noOfNodes = Integer.parseInt(nodesNumber);
			resultSet = new ArrayList<>();
			queryNodes = new HashMap<>();
			queryEdges = new HashMap<>();
			searchSpace = new HashMap<>();
			defaultSearchSpace = new HashMap<>();
			queryNodeList = new ArrayList<>();
			queryEdgeNodes = new HashMap<>();
			for(int i = 0; i < noOfNodes; i++){
				String line = s.nextLine();
				String tokens[] = line.split(" ");
				long nodeId = Long.parseLong(tokens[0]);
				queryNodes.put(nodeId, tokens[1]);
				queryEdges.put(nodeId, new ArrayList<>());
				queryEdgeNodes.put(nodeId, new HashSet<>());
				queryNodeList.add(nodeId);
				searchSpace.put(nodeId, new HashSet<>());
				defaultSearchSpace.put(nodeId, new HashSet<>());
			}
			while(s.hasNextLine()){
				String token = s.nextLine();
				Integer noOfEdges = Integer.parseInt(token);
				for(int i = 0; i < noOfEdges; i++){
					String tokens[] = s.nextLine().split(" ");
					long no1 = Long.parseLong(tokens[0]);
					long no2 = Long.parseLong(tokens[1]);
					queryEdges.get(no1).add(queryNodes.get(no2));
					queryEdgeNodes.get(no1).add(no2);
				}					
			}
			s.close();
		} catch (FileNotFoundException e) {
				e.printStackTrace();
		}			
	}

	private Entities getEntity(String tableName){
		switch(tableName.trim()){
			case "actor":
				return Entities.actor;
			case "director":
				return Entities.director;
			case "movie":
				return Entities.movie;
			case "genre":
				return Entities.genre;
			default:
				return null;
		}
	}


	/**
	 * Performing search space truncation based on label profiles
	 * @param dir :  directory of database
	 */
	private void queryNeo4j(File dir) {
		db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dir).setConfig(GraphDatabaseSettings.pagecache_memory, "512M" ).setConfig(GraphDatabaseSettings.string_block_size, "60" ).setConfig(GraphDatabaseSettings.array_block_size, "300" ).newGraphDatabase();
		Transaction tx = db.beginTx();
		System.out.println("S:"+queryNodes.keySet().size());
		for(Long id: queryNodes.keySet()){
			ArrayList<String>  set = queryEdges.get(id);
			ResourceIterator<Node> nodes = db.findNodes(Label.label(queryNodes.get(id)));
			while(nodes.hasNext()){
				Node node = nodes.next();
				String[] profileNodes = (String[]) node.getProperty("profile");
				if(Arrays.asList(profileNodes).containsAll(set)){
					searchSpace.get(id).add(node.getId());
				}
				defaultSearchSpace.get(id).add(node.getId());
			}
			System.out.print(id+" ,"+searchSpace.get(id).size()+";");			
		}
		newSearchSpace = new HashMap<>();
		sortQueryNodeList();
		getSearchOrder(queryNodeList.get(0));			
		queryNodeList = new ArrayList<>();
		queryNodeList.addAll(visitedGreedy);
		System.out.println("Graph QL order");
		testPrintSet(queryNodeList);		
		System.out.println("OUTPUT");
		Long before = System.currentTimeMillis();
		search(0);
		Long after = System.currentTimeMillis();
		System.out.println("Time Taken: "+(after - before));
		tx.close();
	}

	private void testPrintSet(ArrayList<Long> x){
		for(Long temp: x){
			System.out.print(temp+" ");
		}
		System.out.println();
	}


	public String createMatch(ArrayList<Tag> tags){
        String query = "";
        for(Tag tag: tags){
            if(tag instanceof CypherNode)
                query+=tag;
            else{
                query += "--";
                query += tag;
            }
        }
        return query;
    }

	/*
	public void testPrintSet(HashSet<Long> x){
		for(Long temp: x){
			System.out.print(temp+" ");
		}
		System.out.println();
	}*/
	
	/**
	 * Method carried over.. Sort query nodes as per size of profiled search space.
	 * However this implementation uses default search space for sub-graph matching 
	 */
	private void sortQueryNodeList(){
		for(int i = 0; i < queryNodeList.size(); i++){
			for(int j = i; j < queryNodeList.size(); j++){
				if(defaultSearchSpace.get(queryNodeList.get(i)).size() > defaultSearchSpace.get(queryNodeList.get(j)).size()){
					long swap = queryNodeList.get(i);
					queryNodeList.set(i, queryNodeList.get(j));
					queryNodeList.set(j, swap);
				}
			}
		}
		visitedGreedy = new ArrayList<>();
	}
	
	/**
	 * Getting search order by greedy algorithm
	 */
	private void getSearchOrder(long id){
		visitedGreedy.add(id);
		Long nextNode = null;
		double thisCost = Double.MAX_VALUE;
		HashSet<Long> remainingNodeSet = new HashSet<>();
		for(long visited: visitedGreedy){
			remainingNodeSet.addAll(queryEdgeNodes.get(visited));
		}				
		remainingNodeSet.removeAll(visitedGreedy);
		for(long id2: remainingNodeSet){
			double newCost =  defaultSearchSpace.get(id2).size() * Math.pow(gamma, getNumberOfPrevConnections(id2));
			if(!visitedGreedy.contains(id2) && newCost < thisCost){					
				thisCost = newCost;
				nextNode = id2;
			}
		}
		if(visitedGreedy.size() != queryNodeList.size() && nextNode != null){
			getSearchOrder(nextNode);
		}
	}
	
	/**
	 * Checking current nodes connections with previously visited nodes
	 * @param id : query node
	 * @return : number of prev connections
	 */
	private int getNumberOfPrevConnections(long id){
		HashSet<Long> endNodes = new HashSet<>();
		endNodes.addAll(queryEdgeNodes.get(id));
		endNodes.retainAll(visitedGreedy);			
		return endNodes.size();			
	}
	
	
	/**
	 * Recursive back-tracking search algorithm
	 * @param i : index of query node
	 */
	private void search(int i){
		Long nodeLabel = queryNodeList.get(i);
		HashSet<Long> nodes = defaultSearchSpace.get(nodeLabel);
		for(long node: nodes){
			if(!newSearchSpace.values().contains(node) && check(node, i)){
				newSearchSpace.put(queryNodeList.get(i), node);
				if(i < queryNodeList.size()-1){					
					search(i+1);					
				}else{
					resultSet.add(newSearchSpace);					
					printSearchSpace();
				}
				newSearchSpace.remove(queryNodeList.get(i));
			}
		}
	}	
	
	private void printSearchSpace(){
		for(long key: newSearchSpace.keySet()){
			System.out.print(key+":"+newSearchSpace.get(key)+" ");
		}
		System.out.println();
	}
	
	/**
	 * Check if node is valid for current search-space
	 * @param node : querynode
	 * @param index : index in search order
	 * @return : if node is compatible
	 */
	private boolean check(long node, int index){
		for(int i = 0; i < index; i++){
			if(!edgeExists(i, index))
				continue;
			if(!nodeExists(node, i, RelationshipType.withName("connected"))){
				return false;
			}
		}
		return true;
	}
	
	//Check if edge exists in query graph
	private boolean edgeExists(int from, int to){
		return queryEdgeNodes.get(queryNodeList.get(from)).contains(Long.valueOf(to+""));
	}
	
	
	private boolean nodeExists(long nodeID, int i, RelationshipType relationship) {
		Node node = db.getNodeById(nodeID);
		if(!node.hasRelationship(relationship)){
			return false;
		}
		
		long otherNode = newSearchSpace.get(queryNodeList.get(i));		
		
		for(Relationship rel:node.getRelationships(relationship)){
			if(rel.getOtherNode(node).getId() == otherNode)
				return true;
		}
		return false;
	}
			
}
