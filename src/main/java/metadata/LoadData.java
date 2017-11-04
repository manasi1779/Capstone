package metadata;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LoadData {

    GraphDatabaseService db;
    Transaction tx;
    BatchInserter insert;
    File dbpath;
    static HashMap<String, Integer> offsetID = new HashMap();

    public static void main(String[] args) {
        LoadData dbInstance = new LoadData();
        dbInstance.getData();
    }

    static {
        offsetID.put("genre", 1000);
        offsetID.put("movie", 3000000);
        offsetID.put("actor", 7000000);
        offsetID.put("director", 1000000);
    }

    public LoadData(){
        dbpath = new File("D:/databases/connected_graph.db");
    }


    public void getData(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        try {
            Connection con=DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/imdb","root","root");
            loadData(con);
        }catch(SQLException e1){
            e1.printStackTrace();
        }
    }

    public void loadData(Connection con){
        try {
            DatabaseMetaData md = con.getMetaData();
            ResultSet rs  = md.getTables(null, null, "%", null);
            ArrayList<String> relationTables = new ArrayList<String>();
            insert = BatchInserters.inserter(dbpath);
            while(rs.next()){
                if(rs.getString(3).equals("moviegenres") || rs.getString(3).equals("directedby") || rs.getString(3).equals("roles"))
                    relationTables.add(rs.getString(3));
                else{
                    ArrayList<String> columns = getTableDefinition(rs.getString(3), md);
                    loadTableData(rs.getString(3), con.createStatement(), columns);
                }
            }
            createRoleRelationships(con.createStatement());
            createDirectorRelationships(con.createStatement());
            createMovieGenreRelationship(con.createStatement());
            IndexCreator indexCreator = insert.createDeferredSchemaIndex(getEntity("movies"));
            indexCreator.on("id").create();
            indexCreator = insert.createDeferredSchemaIndex(getEntity("directors"));
            indexCreator.on("id").create();
            indexCreator = insert.createDeferredSchemaIndex(getEntity("actors"));
            indexCreator.on("id").create();
            insert.shutdown();
            removeNodes();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Entities getEntity(String tableName){
        switch(tableName){
            case "actors":
                return Entities.actor;
            case "directors":
                return Entities.director;
            case "movies":
                return Entities.movie;
            default:
                return Entities.genre;
        }
    }

    private void loadTableData(String tableName, Statement createStatement, ArrayList<String> tableDefinition) {
        try (ResultSet rs = createStatement.executeQuery("select * from "+tableName+";")){
            System.out.println("Adding data to "+tableName);
            while(rs.next()){

                HashMap<String, Object> nodeMap = new HashMap();
                long id = 0;
                for(String column: tableDefinition){
                    Object value = rs.getObject(column);
                    if(column.equals("id")){
                        id = convertID((int)value, tableName.substring(0, tableName.length()-1));
                    }
                    else{
                        nodeMap.put(column, value);
                    }
                }
                insert.createNode(id, nodeMap, getEntity(tableName));
            }
            createStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createRoleRelationships(Statement createStatement){
        int i = 0;
        try(ResultSet rs = createStatement.executeQuery("select * from roles;")) {
            while(rs.next()){
                i++;
                if(i == 1000)
                    break;
                Map<String, Object> params = new HashMap<>();
                if(rs.getString("role") != null)
                    params.put("role", rs.getString("role"));
                else
                    params.put("role", "NA");
                if(insert.nodeExists(rs.getLong("actorid"))&&insert.nodeExists(rs.getLong("movieid")))
                    insert.createRelationship(offsetID.get("actor")+rs.getLong("actorid"), offsetID.get("movie")+rs.getLong("movieid"), RelationshipIMDB.acted_in, params);
            }
            createStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createDirectorRelationships(Statement createStatement){
        int i = 0;
        try(ResultSet rs = createStatement.executeQuery("select * from directedby;")){
            while(rs.next()){
                i++;
                if(i == 1000)
                    break;
                long director_id =offsetID.get("director")+rs.getLong("directorid");
                long movie_id = offsetID.get("movie")+rs.getLong("movieid");
                if(insert.nodeExists(director_id)&&insert.nodeExists(movie_id))
                    insert.createRelationship(director_id,movie_id, RelationshipIMDB.directed_by, null);
            }
            createStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createMovieGenreRelationship(Statement createStatement){
        int i = 0;
        try(ResultSet rs = createStatement.executeQuery("select * from moviegenres;")){
            while(rs.next()){
                i++;
                if(i == 1000)
                    break;
                long movie_id = offsetID.get("movie")+rs.getLong("movieid");
                long genre_id = offsetID.get("genre")+rs.getLong("genreid");
                if(insert.nodeExists(movie_id)&& insert.nodeExists(genre_id))
                    insert.createRelationship(movie_id, genre_id, RelationshipIMDB.movie_genre, null);
            }
            createStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to get table definition
     * @param tableName
     * @param md
     * @return
     */
    public ArrayList<String> getTableDefinition(String tableName, DatabaseMetaData md){
        ArrayList<String> tableDefinition = new ArrayList<String>();
        try(ResultSet rs = md.getColumns(null, null, tableName, "%")){
            while(rs.next()){
                tableDefinition.add(rs.getString(4));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tableDefinition;
    }

    public long convertID(int id, String type){
        long offset = offsetID.get(type);
        return offset+id;
    }

    /*
    match (n) where not (n)--() delete (n)
     */
    public void removeNodes(){
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbpath).setConfig(GraphDatabaseSettings.pagecache_memory, "2G" ).setConfig(GraphDatabaseSettings.string_block_size, "60" ).setConfig(GraphDatabaseSettings.array_block_size, "50" ).newGraphDatabase();
        Transaction tx = db.beginTx();
        ResourceIterable<Node> nodes =  db.getAllNodes();
        for(Node node: nodes){
            if(!node.hasRelationship())
                node.delete();
            tx.success();
        }
        tx.close();
    }
}
