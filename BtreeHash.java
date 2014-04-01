import com.sleepycat.db.*;

import java.util.*;

public class BtreeHash {
	private static final String BTREEDB = "/tmp/maviskay_db/Btree_db";
	private static final String HASHDB = "/tmp/maviskay_db/Hash_db";
	private static final int recordsCount = 1000;

    // Create the database
	public static Database create(String option) {
		Database db = null;
		// If database is not already created
			try {
				DatabaseConfig dbConfig = new DatabaseConfig();
				// Create btree database
				if (option.equalsIgnoreCase("btree")) {
					dbConfig.setType(DatabaseType.BTREE);
					//dbConfig.setExclusiveCreate(true);
					dbConfig.setAllowCreate(true);
					db = new Database(BTREEDB, null, dbConfig);
					System.out.println(BTREEDB + " has been created");
				// Create hash database
				} else if (option.equalsIgnoreCase("hash")) {
					dbConfig.setType(DatabaseType.HASH);
					//dbConfig.setExclusiveCreate(true);
					dbConfig.setAllowCreate(true);
					db = new Database(HASHDB, null, dbConfig);
					System.out.println(HASHDB + " has been created");
				}
				// Populate database
				populate(db, recordsCount);
				System.out.println("1000 records inserted");
				db.close();
				return db;
			} catch (Exception e) {
				e.printStackTrace();
			}
		return db;
	}
	
	// Populate the database with random key-data pair
	public static void populate(Database db, int count) {
		Random random = new Random(1000000);
		DatabaseEntry kdbt, ddbt;
		int range;
		String key, data;
		
		try {
            for (int i = 0; i < count; i++) {
            	/* to generate a key string */
        		range = 64 + random.nextInt( 64 );
        		key = "";
        		for ( int j = 0; j < range; j++ ) 
        			key+=(new Character((char)(97+random.nextInt(26)))).toString();
        		
        		/* to create a DBT for key */
        		kdbt = new DatabaseEntry(key.getBytes());
        		kdbt.setSize(key.length()); 
        		
        		/* to generate a data string */
        		range = 64 + random.nextInt(64);
        		data = "";
        		for ( int j = 0; j < range; j++ ) 
        			  data+=(new Character((char)(97+random.nextInt(26)))).toString();
        		
        		/* to create a DBT for data */
        		ddbt = new DatabaseEntry(data.getBytes());
        		ddbt.setSize(data.length()); 
        		
        		/* to insert the key/data pair into the database */
                if (db.putNoOverwrite(null, kdbt, ddbt) == OperationStatus.KEYEXIST) {
                	System.out.println("Key already exists\n");
                }
            }
        } catch (DatabaseException dbe) {
        	System.err.println("Populate the table: " + dbe.toString());
        }
	}
	
	// Searches the database by key, data, or range
	public static void search(Database db, String option, int type) {
		if (db == null) {
			System.out.println("Please create the database first\n");
			return;
		}
		while (true) {
			Scanner keyboard = new Scanner(System.in);
			// By key
			if (type == 2) {
				System.out.print("Enter a key to search: ");
				String key = keyboard.nextLine();
				if(isValid(key)) {
					// Searches database by specified key - returns if key-data pair is found
					if(searchByKeyData(db, key, "key")) {
						return;
					}
				}
			// By data
			} else if (type == 3) {
				System.out.print("Enter a data value to search: ");
				String data = keyboard.nextLine();
				if(isValid(data)) {
					// Searches database by specified data - returns if key-data pair is found
					if(searchByKeyData(db, data, "data")) {
						return;
					}
				}
			// By range of keys
			} else if (type == 4) {
				System.out.print("Enter a lower bound key to search: ");
				String lowerKey = keyboard.nextLine();
				System.out.print("Enter a upper bound key to search: ");
				String upperKey = keyboard.nextLine();
				if(isValid(lowerKey) && isValid(upperKey)) {
					if(lowerKey.compareTo(upperKey) < 0) {
						if(searchByKeyRange(db, lowerKey, upperKey)) {
							
						}
					} else {
						System.out.print("Lower bound key must be smaller than upper bound key. Please try again: ");
					}
				}
			}	
		}
	}
	
	// Searches the database by the specified key or data value
	public static boolean searchByKeyData(Database db, String inputString, String type) {
		try {
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();
			if (type.equalsIgnoreCase("key")){
				key.setData(inputString.getBytes());
				key.setSize(inputString.length());
			} else if (type.equalsIgnoreCase("data")) {
				data.setData(inputString.getBytes());
				data.setSize(inputString.length());
			}
			long startTime = System.nanoTime();
			// Key-data pair found
			if (db.get(null, key, data, null) == OperationStatus.SUCCESS) {
				long totalTime = (System.nanoTime() - startTime) / 1000;
				System.out.println(db.getDatabaseName() + " took "+ totalTime + " microseconds to search by " + type);
				System.out.println("The key - data pair is: " + key.toString() + " - " + data.toString());
				return true;
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		// Fallthrough case
		System.out.println("No matching " + type + " was not found");
		return false;
	}
	
	// Searches the database by the range of key values
	public static boolean searchByKeyRange(Database db, String lower, String upper) {
		
		// Fallthrough case
		System.out.println("No data was found within the range " + lower + " & " + upper);
		return false;
	}
	
	// Check if the user input is valid
	public static boolean isValid(String input) {
		// Check if valid key or data
		if (input.length() < 64 || input.length() > 127) {
			System.out.print("Length of input invalid, please try again: ");
			return false;
		}
		// Check if input contains only alphabets
		char[] characters = input.toCharArray();
		for (char c : characters){
			if (!Character.isLetter(c)) {
				System.out.print("Input must only be roman characters. Please try again: ");
				return false;
			}
		}
		// Check if input is all lower case
		if (!input.equals(input.toLowerCase())) {
			System.out.print("Input must be in lowercase. Please try again: ");
			return false;
		}
		return true;
	}
}