import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import com.sleepycat.db.*;

public class Indexfile {
	private static final String INDEXDB = "/tmp/maviskay_db/Index_db";
	private static final String INDEXDBREV = "/tmp/maviskay_db/IndexRev_db";
	private static final int recordsCount = 100000;
	
	// Create the database
	public static List<Database> create(Database db, Database dbIndex, String dbType) {
		// Create list of db to return
		List<Database> dbList = new ArrayList<Database>();
		if (db == null && dbIndex == null) {
			// If database is not already created, create index file database
				try {
					// Set up database
					DatabaseConfig dbConfig = new DatabaseConfig();
					dbConfig.setType(DatabaseType.BTREE);
					dbConfig.setAllowCreate(true);
					db = new Database(INDEXDB, null, dbConfig);
					// Set up indexing
					DatabaseConfig dbConfigIndex =  new DatabaseConfig();
					dbConfigIndex.setType(DatabaseType.BTREE);
					dbConfigIndex.setAllowCreate(true);
					dbIndex = new Database(INDEXDBREV, null, dbConfigIndex);
					// Populate database
					System.out.println(dbType + " database & it's indexing has been created, populating database");
					populate(db, dbIndex, recordsCount);
					System.out.println(recordsCount + " records inserted\n");
					// Add the database to list for returning
					dbList.add(db);
					dbList.add(dbIndex);
					return dbList;
				} catch (Exception e) {
					e.printStackTrace();
				}
		} else {
			System.out.println (dbType + " database already exists\n");
		}	
		return dbList;
	}
	
	// Populate the database with random key-data pair
	public static void populate(Database db, Database dbIndex, int count) {
		Random random = new Random(1000000);
		DatabaseEntry kdbt, ddbt, kdbtIndex, ddbtIndex;
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
                if (db.putNoOverwrite(null, kdbt, ddbt) != OperationStatus.KEYEXIST) {
                	kdbtIndex = new DatabaseEntry(data.getBytes());
                	kdbtIndex.setSize(data.length());
                	ddbtIndex = new DatabaseEntry(key.getBytes());
                	ddbtIndex.setSize(key.length());
                	if(dbIndex.put(null, kdbtIndex, ddbtIndex) != OperationStatus.SUCCESS) {
                		System.out.println("Database indexing could not be completed");
                	}
                }
            } 
        } catch (DatabaseException dbe) {
        	System.err.println("Populate the table: " + dbe.toString());
        }
	}
	
	// Searches the database by key, data, or range
	public static void search(Database db, Database dbIndex, String dbType, int searchType) {
		if (db == null || dbIndex == null) {
			System.out.println("Please create the database & it's indexing first\n");
			return;
		}
		while (true) {
			Scanner keyboard = new Scanner(System.in);
			// By key
			if (searchType == 2) {
				System.out.print("Enter a key to search (-1 to return): ");
				String key = keyboard.nextLine();
				if(BtreeHash.checkReturn(key)){
					return;
				}
				if(BtreeHash.isValid(key)) {
					// Searches database by specified key - returns if key-data pair is found
					if(BtreeHash.searchByKeyData(db, key, "key", dbType)) {
						return;
					}
				}
			// By data
			} else if (searchType == 3) {
				System.out.print("Enter a data value to search (-1 to return): ");
				String data = keyboard.nextLine();
				if(BtreeHash.checkReturn(data)){
					return;
				}
				if(BtreeHash.isValid(data)) {
					// Searches database by specified data - returns if key-data pair is found
					if(BtreeHash.searchByKeyData(dbIndex, data, "key", dbType)) {
						return;
					}
				}
			// By range of keys
			} else if (searchType == 4) {
				System.out.print("Enter a lower bound key to search (-1 to return): ");
				String lowerKey = keyboard.nextLine();
				if(BtreeHash.checkReturn(lowerKey)){
					return;
				}
				System.out.print("Enter a upper bound key to search (-1 to return): ");
				String upperKey = keyboard.nextLine();
				if(BtreeHash.checkReturn(upperKey)){
					return;
				}
				// Searches database by key bounds - returns if key-data pair is found
				if(BtreeHash.isValid(lowerKey) && BtreeHash.isValid(upperKey)) {
					if(lowerKey.length() <= upperKey.length() && lowerKey.compareTo(upperKey) < 0) {
						if(BtreeHash.searchByKeyRange(db, lowerKey, upperKey, dbType)) {
							return;
						}
					} else {
						System.out.print("Lower bound key must be smaller than upper bound key");
					}
				}
			}
		}
	}
}
