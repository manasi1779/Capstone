import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import java.io.File;
import java.util.*;

public class Profiling {
	
	String dbpath;
	Transaction tx;
	BatchInserter insert;
	GraphDatabaseService db;
	HashMap<String, HashSet<Long>> searchSpaces = new HashMap();

	public static void main(String[] args) {
		Profiling profiling = new Profiling();
		profiling.loadData();
	}

	public Profiling(){
		dbpath = "D:/neo4j-enterprise-3.1.1-windows/neo4j-enterprise-3.1.1/data/databases/graph.db";
	}
	
	public void loadData(){
		//File file = new File("C:/Manasi/workspace/TDM/Graph/data/Proteins/Proteins/target");
		File file = new File("C:/Manasi/workspace/TDM/Graph/data/iGraph/target");
		listFilesForFolder(file);
	}

	public void listFilesForFolder(final File folder) {
	    for(final File fileEntry : folder.listFiles()){
	    	getDefaultSearchSpace(fileEntry);
	    }	    
	}

	// Get search space for label match
	public void getDefaultSearchSpace(File fileEntry){
		int dotIndex = fileEntry.getName().lastIndexOf('.');
		String newDir = fileEntry.getName().substring(0, dotIndex); 
		System.out.println("Adding profiles to: "+newDir);
		newDir = dbpath+"/"+newDir;
		File dir = new File(newDir);		
		db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dir).setConfig(GraphDatabaseSettings.pagecache_memory, "512M" ).setConfig(GraphDatabaseSettings.string_block_size, "60" ).setConfig(GraphDatabaseSettings.array_block_size, "300" ).newGraphDatabase();
		tx = db.beginTx();
		addProfiles();
		tx.success();
		tx.close();
	}
	
	public void addProfiles(){
		ResourceIterable<Node> nodes = db.getAllNodes();
		for(Node node: nodes){
			Iterable<Relationship> relationships = node.getRelationships();
			ArrayList<String> endNodes = new ArrayList<String>();
			Iterator<Label> ownlabels = node.getLabels().iterator();
			while(ownlabels.hasNext()){
				endNodes.add(ownlabels.next().name());
			}
			for(Relationship relation: relationships){
				Iterator<Label> labels = relation.getOtherNode(node).getLabels().iterator();				
				while(labels.hasNext()){
					endNodes.add(labels.next().name());
				}
			}
			Collections.sort(endNodes);			
			String[] profileNodes = new String[0];
			if(endNodes.size() != 0){				
				profileNodes = endNodes.toArray(profileNodes);
				node.setProperty("profile", profileNodes);
			}
		}		
	}	
}
