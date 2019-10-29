package com.test.friend_suggestions;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;


import static org.neo4j.driver.v1.Values.parameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class FriendSuggestion implements AutoCloseable {

	private final Driver driver;
    private static GraphDatabaseService graphDb;

    private static final String dbPath = "C:\\Users\\rohit\\Desktop\\neo4j-community-3.5.11-windows\\neo4j-community-3.5.11\\database";
    private static final String txtFile = "C:\\Users\\rohit\\Desktop\\neo4j-community-3.5.11-windows\\neo4j-community-3.5.11\\import\\sample.txt";
    
    private static BufferedReader br;
    
    public FriendSuggestion( String uri, String user, String password )
    {
		driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    public void close() throws Exception
    {
        driver.close();
    }

    private enum relationTypes implements RelationshipType
    {
        FRIEND
    }
    
    private enum myLabels implements Label
    {
        USER
    }
    
    private int createNode(Transaction tx, String user)
    {
    	tx.run("MERGE (user:USER {name: $name})", parameters("name", user));
    	return 1;
    }
    
    private int createRelationship(Transaction tx, String user1, String user2)
    {
    	
//    	MERGE (user1:USER {name: row.user1})
//    	MERGE (user2:USER {name: row.user2})
//    	MERGE (user1)-[:FRIEND]-(user2)   
    	
    	tx.run("MERGE (user1:USER {name:$user1})"
    			+ "MERGE (user2:USER {name:$user2})"
    			+ "MERGE (user1)-[:FRIEND]-(user2)", parameters("user1", user1, "user2", user2));
    	
//    	tx.run("MERGE (user1:USER {name:$user1})-[:FRIEND]-(user2:USER {name:$user2})", 
//    			parameters("user1", user1, "user2", user2));
    	return 1;
    }
    
	private void createDatabase()
    {
   	
    	GraphDatabaseFactory graphDbFactory = new GraphDatabaseFactory();
    	graphDb = graphDbFactory.newEmbeddedDatabase(new File(dbPath));
    	
    	try {
			br = new BufferedReader(new FileReader(new File(txtFile)));
			String line = br.readLine(); 
			
			  while ((line = br.readLine()) != null) {
			    System.out.println(line); 
			    
			    final String[] splitted = line.split(" ");
			    
		    	try( Session session = driver.session() ) {	

		    		session.writeTransaction(tx -> createNode(tx, splitted[0]));
		    		session.writeTransaction(tx -> createNode(tx, splitted[1]));
		    		session.writeTransaction(tx -> createRelationship(tx, splitted[0], splitted[1]));
			    	
			    } catch (Exception e) {e.printStackTrace();} 
			    
			  } 
		
		} catch (Exception e) {e.printStackTrace();}
 	
    }
	
	private int findSuggestions(Transaction tx, String name) 
	{
		
//    	MATCH(user:USER {name:"0"})–[:FRIEND]-(my_friends)–[:FRIEND]-(friends_of_my_friends) 
//    	WHERE NOT((user)–[:FRIEND]–(friends_of_my_friends)) 
//    	RETURN collect(my_friends.name), friends_of_my_friends.name
		
		HashMap<String, List> hm = new HashMap<String, List>();
		
		StatementResult result = tx.run("MATCH (user:USER {name:$name})-[:FRIEND]-(my_friends)-[:FRIEND]-(friends_of_my_friends)" + 
							" WHERE NOT(user)-[:FRIEND]-(friends_of_my_friends)" +
							" RETURN collect(my_friends.name) AS SuggestionsFrom, friends_of_my_friends AS FriendSuggested",
							parameters("name", name));
		
		System.out.println();
		
		while( result.hasNext() )
		{
			Record record = result.next();
			
			List<Object> from = record.get("SuggestionsFrom").asList();
			String suggested = record.get( "FriendSuggested" ).get("name").asString();
			
			System.out.println( from + " suggested this guy : " + suggested );
			
			hm.put(suggested, from);
			
//			for(int i=0; i<list.size(); i++)
//			{
//				hm.merge((String) list.get(i), 1, (a, b) -> a + b);
//			}
		}
		
//		hm.entrySet().forEach(entry -> {
//		    System.out.println(entry.getKey() + " " + entry.getValue());  
//		 });
		
		System.out.println();
		System.out.println("SORTED as per the MAX SUGGESTIONS FROM");
		
		String maxKey = Collections.max(hm.entrySet(), (k1, k2) -> k1.getValue().size() - k2.getValue().size()).getKey();
		int maxSize = hm.get(maxKey).size();
		
		hm.entrySet().stream()
        .sorted((k1, k2) -> k2.getValue().size() - k1.getValue().size())
        .forEach(k -> {
        	int size = k.getValue().size();
        	if(maxSize == size)
        		System.out.println("Suggested user: "+k.getKey());
//        	System.out.println(k.getKey() + ": " + k.getValue());
        });
		
		System.out.println();
	

		return 1;
			
	}

	
	private void displaySuggestions(String name)
	{
		try(Session session = driver.session())
		{
			session.readTransaction(tx -> findSuggestions(tx, name));
		}
	}
	
	private int removeNR(Transaction tx)
	{
		tx.run("MATCH (n)"+
				"DETACH DELETE n");
		return 1;
	}
	
	private void deleteDatabase()
	{
		try( Session session = driver.session() )
		{
			session.writeTransaction(tx -> removeNR(tx));
		}
	}
    
    
	private void run()
    {
        
    	createDatabase();
    	displaySuggestions("0");
//    	deleteDatabase();
    	
   	
        graphDb.shutdown();
        System.out.println("Done");
        
    }

    public static void main( String... args ) throws Exception
    {
        try ( FriendSuggestion fs = new FriendSuggestion( "bolt://localhost:7687", "neo4j", "pwd123" ) )
        {
            fs.run();
        }
    }
	
}



