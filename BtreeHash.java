import com.sleepycat.db.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class BtreeHash {
	private static final String BTREEDB = "/tmp/maviskay_db/Btree_db";
	private static final String HASHDB = "/tmp/maviskay_db/Hash_db";
	private static final int recordsCount = 100000;

    // Create the database
	public static Database create(Database db, String dbType) {
		if (db == null) {
			// If database is not already created
				try {
					DatabaseConfig dbConfig = new DatabaseConfig();
					// Create btree database
					if (dbType.equalsIgnoreCase("btree")) {
						dbConfig.setType(DatabaseType.BTREE);
						dbConfig.setAllowCreate(true);
						db = new Database(BTREEDB, null, dbConfig);
					// Create hash database
					} else if (dbType.equalsIgnoreCase("hash")) {
						dbConfig.setType(DatabaseType.HASH);
						dbConfig.setAllowCreate(true);
						db = new Database(HASHDB, null, dbConfig);
					}
					// Populate database
					System.out.println(dbType + " database has been created");
					populate(db, recordsCount);
					System.out.println(recordsCount + " records inserted\n");
					return db;
				} catch (Exception e) {
					e.printStackTrace();
				}
		} else {
			System.out.println (dbType + " database already exists\n");
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
	public static void search(Database db, String dbType, int searchType) {
		if (db == null) {
			System.out.println("Please create the database first\n");
			return;
		}
		while (true) {
			Scanner keyboard = new Scanner(System.in);
			// By key
			if (searchType == 2) {
				System.out.print("Enter a key to search (-1 to return): ");
				String key = keyboard.nextLine();
				if(checkReturn(key)){
					return;
				}
				if(isValid(key)) {
					// Searches database by specified key - returns if key-data pair is found
					if(searchByKeyData(db, key, "key", dbType)) {
						return;
					}
				}
			// By data
			} else if (searchType == 3) {
				System.out.print("Enter a data value to search (-1 to return): ");
				String data = keyboard.nextLine();
				if(checkReturn(data)){
					return;
				}
				if(isValid(data)) {
					// Searches database by specified data - returns if key-data pair is found
					if(searchByKeyData(db, data, "data", dbType)) {
						return;
					}
				}
			// By range of keys
			} else if (searchType == 4) {
				System.out.print("Enter a lower bound key to search (-1 to return): ");
				String lowerKey = keyboard.nextLine();
				if(checkReturn(lowerKey)){
					return;
				}
				System.out.print("Enter a upper bound key to search (-1 to return): ");
				String upperKey = keyboard.nextLine();
				if(checkReturn(upperKey)){
					return;
				}
				// Searches database by key bounds - returns if key-data pair is found
				if(isValid(lowerKey) && isValid(upperKey)) {
					if(lowerKey.compareTo(upperKey) < 0) {
						if(searchByKeyRange(db, lowerKey, upperKey, dbType)) {
							return;
						} else {
							System.out.println("No data was found within the range " + lowerKey + " & " + upperKey);
						}	
					} else {
						System.out.print("Lower bound key must be smaller than upper bound key");
					}
				}
			}	
		}
	}
	
	// Searches the database by the specified key or data value
	public static boolean searchByKeyData(Database db, String inputString, String searchType, String dbType) {
		try {
			long startTime = 0;
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();
			// Search by key
			if (searchType.equalsIgnoreCase("key")){
				key.setData(inputString.getBytes());
				key.setSize(inputString.length());
				startTime = System.nanoTime();
				// Key-data pair found
				if (db.get(null, key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					long totalTime = (System.nanoTime() - startTime) / 1000;
					System.out.println(dbType + " database took "+ totalTime + " microseconds to search by " + searchType);
					String keyString = new String(key.getData());
					String dataString = new String(data.getData());
					writeToFile(keyString, dataString);
					System.out.println("The key - data pair is:\n " + "\tkey:\t" + keyString + "\n\tdata:\t" + dataString + "\n");
					return true;
				}
			// Search by data
			} else if (searchType.equalsIgnoreCase("data")) {	
				int count = 0;
				Cursor dbCursorData = db.openCursor(null, null);
				data.setData(inputString.getBytes());
				data.setSize(inputString.length());
				startTime = System.nanoTime();
				dbCursorData.getFirst(key, data, LockMode.DEFAULT);
				do {
					if (inputString.equals(new String(data.getData()))) {
						String keyString = new String(key.getData());
						String dataString = new String(data.getData());
						writeToFile(keyString, dataString);
						System.out.println("The key - data pair is:\n " + "\tkey:\t" + keyString + "\n\tdata:\t" + dataString);
						count++;
					}
					data.setData(inputString.getBytes());
					data.setSize(inputString.length());
				} while (dbCursorData.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS);
				// Key-data pairs found
				if (count != 0) {
					long totalTime = (System.nanoTime() - startTime) / 1000;
					System.out.println(dbType + " database took "+ totalTime + " microseconds to search by " + searchType);
					System.out.println(" \t" + count + " results were found\n");
					return true;
				}
			}
			// No key-data pairs found
			long totalTime = (System.nanoTime() - startTime) / 1000;
			System.out.println(dbType + " database took "+ totalTime + " microseconds to search by " + searchType);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		// Fallthrough case
		System.out.println("No matching " + searchType + " was not found\n");
		return false;
	}
	
	// Searches the database by the range of key values
	public static boolean searchByKeyRange(Database db, String lower, String upper, String dbType) {
		boolean rtn = false;
		try {
			long startTime = 0;
			Cursor dbCursor = db.openCursor(null, null);
			DatabaseEntry lowerKey = new DatabaseEntry();
			DatabaseEntry upperKey = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();
			DatabaseEntry key = lowerKey;
			lowerKey.setData(lower.getBytes());
			lowerKey.setSize(lower.length());
			upperKey.setData(upper.getBytes());
			upperKey.setSize(upper.length());
			
			dbCursor.getSearchKey(key, data, LockMode.DEFAULT);	
			startTime = System.nanoTime();
			while (key != upperKey) {
				if (db.get(null, key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
					rtn = true;
					String keyString = new String(key.getData());
					String dataString = new String(data.getData());
					writeToFile(keyString, dataString);
					System.out.println("The key - data pair is:\n " + "\tkey:\t" + keyString + "\n\tdata:\t" + dataString);
				}
				dbCursor.getNext(key, data, LockMode.DEFAULT);
			}
			long totalTime = (System.nanoTime() - startTime) / 1000;
			System.out.println(dbType + " database took "+ totalTime + " microseconds to search by key range");
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		return rtn;
	}
	
	// Check if the user input is valid
	public static boolean isValid(String input) {
		// Check if valid key or data
		if (input.length() < 64 || input.length() > 127) {
			System.out.println("Length of input invalid");
			return false;
		}
		// Check if input contains only alphabets
		char[] characters = input.toCharArray();
		for (char c : characters){
			if (!Character.isLetter(c)) {
				System.out.println("Input must only be roman characters");
				return false;
			}
		}
		// Check if input is all lower case
		if (!input.equals(input.toLowerCase())) {
			System.out.println("Input must be in lowercase");
			return false;
		}
		return true;
	}
	
	// Write the search results to the answers.txt file 
	public static void writeToFile(String key, String data) {
		try {
			PrintWriter answers;
			answers = new PrintWriter(new BufferedWriter(new FileWriter("answers.txt", true)));

			answers.println("Key: " + key);
			answers.println("Data: " + data);
			answers.println(" ");
			answers.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Check if user wishes to go back a step
	public static boolean checkReturn(String input){
		if(input.equals("-1")){
			return true;
		} else {
			return false;
		}
	}
}

