import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Program {

	/**
	 * Main program.
	 * 
	 * @param args Command-line arguments. Not used here.
	 */
	public static void main(String[] args) {

		Program prog = new Program();
		
		DB db = null;
		
		try {
			db = prog.connectToDb();
		} catch (UnknownHostException e) {
			System.out.println("ERROR: Unknown host");
			System.exit(1);
		}
		
		System.out.println("-----");
		System.out.println("Dropping people collection");
		prog.clearPeople(db);
		System.out.println("-----");
		System.out.println("People in database: " + prog.getPeopleCount(db));
		System.out.println("-----");
		System.out.println("Adding three people to people collection");
		prog.insertSomeRecords(db);
		System.out.println("-----");
		System.out.println("People in database: " + prog.getPeopleCount(db));
		System.out.println("-----");
		System.out.println("Displaying contents of people collection");
		List<DBObject> peopleList = prog.getPeople(db);
		System.out.println("");
		
		for (DBObject person : peopleList) {
		    System.out.println("  Name: " + person.get("name"));
		    System.out.println("  Type: " + person.get("type"));
		    System.out.println("  Age:  " + person.get("age"));
		    System.out.println("");
		}
		
		System.out.println("-----");
	}
	
	/**
	 * Establishes a connection to the local MongoDB database.
	 * @return A handle to the MongoDB database.
	 * @throws UnknownHostException  If the host could not be found.
	 */
	public DB connectToDb() throws UnknownHostException {
		MongoClient mongoClient;
		mongoClient = new MongoClient("localhost");
		DB db = mongoClient.getDB("myfirstmongodb");
		return db;
	}
	
	/**
	 * Returns the current number of people stored inside the 
	 * people collection.
	 * 
	 * @param db A handle to the MongoDB database.
	 * @return   People count.
	 */
	public long getPeopleCount(DB db) {
		DBCollection people = db.getCollection("people");
		return people.count();
	}
	
	/**
	 * Removes the people collection from the database.
	 * 
	 * @param db A handle to the MongoDB database.
	 */
	public void clearPeople(DB db) {
		DBCollection people = db.getCollection("people");
		people.drop();
	}
	
	/**
	 * Inserts some test records into the people collection.
	 * 
	 * @param db A handle to the MongoDB database.
	 */
	public void insertSomeRecords(DB db) {
		DBCollection people = db.getCollection("people");

		// Add a new person named "Steve"
		BasicDBObject steve = new BasicDBObject();
		steve.put("name", "Steve");
		steve.put("type", "Developer");
		steve.put("age", "26");
		people.insert(steve);
		
		// Add a new person named "Eve"
		BasicDBObject eve = new BasicDBObject();
		eve.put("name", "Eve");
		eve.put("type", "Project Manager");
		eve.put("age", "35");
		people.insert(eve);
		
		// Add a new person named "John"
		BasicDBObject john = new BasicDBObject();
		john.put("name", "John");
		john.put("type", "IT Administrator");
		john.put("age", "32");
		people.insert(john);
	}
	
	/**
	 * Returns a list of people currently stored inside
	 * the people collection.
	 * 
	 * @param db A handle to the MongoDB database.
	 * @return   A list of people.
	 */
	public List<DBObject> getPeople(DB db) {
		List<DBObject> peopleList = new ArrayList<DBObject>();
		
		DBCollection people = db.getCollection("people");
		DBCursor cursor = people.find();
		
		try {
		   while(cursor.hasNext()) {
		       peopleList.add(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
		
		return peopleList;
	}
}
