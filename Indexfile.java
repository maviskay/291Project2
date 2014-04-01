import com.sleepycat.db.*;

public class Indexfile {
	private static Database db = null;
	// Create the database
	public static Database create(String option) {
		
		return db;
	}
	
	// Populate the database with random key-data pair
	public static void populate(Database db, int count) {
		
	}
	
	// Searches the database by key, data, or range
	public static void search(Database db, int type) {
		// By key
		if (type == 2) {
			
		// By data
		} else if (type == 3) {
			
		// By range
		} else if (type == 4) {
			
		}
	}
}