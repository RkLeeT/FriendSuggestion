package com.test.friend_suggestions;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;


import static org.neo4j.driver.v1.Values.parameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FriendSuggestion implements AutoCloseable {

	private final Driver driver;
    private final Relationship relation;
    private static Session session;
    private static GraphDatabaseService graphDb;
    private static Transaction transaction;

    private static final String dbPath = "C:\\Users\\rohit\\Desktop\\neo4j-community-3.5.11-windows\\neo4j-community-3.5.11\\database";
    private static final String txtFile = "C:\\Users\\rohit\\Desktop\\neo4j-community-3.5.11-windows\\neo4j-community-3.5.11\\import\\sample.txt";
    
    private static BufferedReader br;
    
    public FriendSuggestion( String uri, String user, String password )
    {
        this.graphDb = null;
		this.relation = null;
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
    
    private <T extends PropertyContainer> T setProperties( final T entity, final Object[] properties )
    {
        for ( int i = 0; i < properties.length; i++ )
        {
            String key = properties[i++].toString();
            Object value = properties[i];
            entity.setProperty( key, value );
        }
        return entity;
    }
    
    private Node createNode( final Object... properties )
    {
        return setProperties( graphDb.createNode(), properties );
    }
    
    private Relationship createRelationship( final Node start, final Node end)
    {
        return start.createRelationshipTo(end, relationTypes.FRIEND );
    }
    
	private void createDatabase()
    {
    	Node user1, user2;
    	Relationship relation;
    	
    	GraphDatabaseFactory graphDbFactory = new GraphDatabaseFactory();
    	graphDb = graphDbFactory.newEmbeddedDatabase(new File(dbPath));
    	
    	try {
			br = new BufferedReader(new FileReader(new File(txtFile)));
			String line; 
			  while ((line = br.readLine()) != null) {
			    System.out.println(line); 
			    
			    final String[] splitted = line.split(" ");
			    
			    try( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() ) {
			    	
	    	
//			    	user1 = createNode("name", splitted[0]);
//				    user2 = createNode("name", splitted[1]);
//				    user1.addLabel(myLabels.USER);
//				    user2.addLabel(myLabels.USER);
				    
//				    relation = createRelationship(user1, user2);
				    
				    Session session = driver.session();
		            session.writeTransaction( new TransactionWork<String>()
		            {
		                public String execute( Transaction tx )
		                {
		                    StatementResult result = tx.run( "MERGE (user1:USER{name:\""+splitted[0]+"\"})",
		                            parameters( splitted[0], "name1" ) );
		                    
		                    result = tx.run( "MERGE (user2:USER{name:\""+splitted[1]+"\"})",
		                                    parameters( splitted[1], "name2" ) );
						                    
		                    result = tx.run( "MERGE (user1:USER{name:\""+splitted[0]+"\"})-[:FRIEND]-(user2:USER{name:\""+splitted[1]+"\"})",
                                    		parameters( splitted[0], "name1", splitted[1], "name2" ));
		                    return "";
		                    
//		                    return result.single().get( 0 ).asString();
				    		
				    		
				    
		                }
		                
		            } );
		            
//		            System.out.println(output);
				    
				    
				    tx.success();
			    	
			    } catch (Exception e) {
					e.printStackTrace();
				} 
			    
			  } 
		
		} catch (Exception e) {
			e.printStackTrace();
		}
 	
    }
    
    
    public void printGreeting( final String message )
    {
        try (Session session = driver.session() )
        {
        	createDatabase();
        	
        	
        	
//        	MERGE (user1:USER {name: row.user1})
//        	MERGE (user2:USER {name: row.user2})
//        	MERGE (user1)-[:FRIEND]-(user2)       
//            
        	
            
            
            
//            String greeting = session.writeTransaction( new TransactionWork<String>()
//            {
//                public String execute( Transaction tx )
//                {
//                    StatementResult result = tx.run( "CREATE (a:Greeting) " +
//                                                     "SET a.message = $message " +
//                                                     "RETURN a.message + ', from node ' + id(a)",
//                            parameters( "message", message ) );
//                    return result.single().get( 0 ).asString();
//                }
//            } );
//            System.out.println( greeting );
            
            
            
            graphDb.shutdown();
            System.out.println("Done");
        }
    }

    public static void main( String... args ) throws Exception
    {
        try ( FriendSuggestion fs = new FriendSuggestion( "bolt://localhost:7687", "neo4j", "pwd123" ) )
        {
            fs.printGreeting( "hello, world" );
        }
    }

	
	
}
