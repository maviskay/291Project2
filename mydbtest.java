// TODO: Implement indexfile database

import com.sleepycat.db.*;
import java.io.*;
import java.util.*;

public class mydbtest {
	private static final String path = "/tmp/maviskay_db";
	private static final String BTREEDB = "/tmp/maviskay_db/Btree_db";
	private static final String HASHDB = "/tmp/maviskay_db/Hash_db";
	
	public static void main(String[] args) {
		// Check if option was selected		
		if (args.length != 1 ){
			System.out.println("Please specify an option");
			System.out.println("\tbtree\n\thash\n\tindexfile\n");
			System.exit(0);
		}

		// Create directory
		boolean createDir = new File(path).mkdir();
		if (!createDir){
			System.out.println("Could not create " + path + " directory");
			System.out.println("Please manually check if the directory is empty before continuing\n");
		}

		// Check user option
		if (args[0].equalsIgnoreCase("btree") || args[0].equalsIgnoreCase("hash") || args[0].equalsIgnoreCase("indexfile")) {
			requestOption(args[0]);
		} else{
			System.out.println("Option selected invalid");
			System.out.println("\tbtree\n\thash\n\tindexfile\n");
			System.exit(0);
		}
	}

	// Requests user for selection
	public static void requestOption(String option) {
		Database db = null;
		
		while (true) {
			System.out.println("Please select an option:");
			System.out.println("\t1. Create and populate the database");
			System.out.println("\t2. Retrieve records with a given key");
			System.out.println("\t3. Retrieve records with a given data");
			System.out.println("\t4. Retrieve records with a given range of key values");
			System.out.println("\t5. Destroy the database");
			System.out.println("\t6. Quit");
			Scanner keyboard = new Scanner(System.in);
			try {
				int selection = keyboard.nextInt();
				// Create & populate database
				if (selection == 1){
					if (option.equalsIgnoreCase("indexfile")) {
						db = Indexfile.create(option);
					} else {
						db = BtreeHash.create(option);
					}
				// Search by key, data, or range
				} else if (selection >= 2 && selection <= 4) {
					if (option.equalsIgnoreCase("indexfile")) {
						Indexfile.search(db, selection);
					} else {
						BtreeHash.search(db, option, selection);
					}
				// Destroy database
				} else if (selection == 5) {
					if (db != null) {
						// Destroy index file database
						if (option.equalsIgnoreCase("indexfile")) {
							
						} else {
							try {
								// Destroy btree database
								if (option.equalsIgnoreCase("btree")) {
									Database.remove(BTREEDB, null, null);
								// Destroy hash database
								} else if (option.equalsIgnoreCase("hash")) {
									Database.remove(HASHDB, null, null);
								}
							} catch (DatabaseException e) {
								e.printStackTrace();
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							}
						}
					} else {
						System.out.println (option + " database does not exist\n");
					}
				// Quit
				} else if (selection == 6) {
					// Removes directory and quits
					boolean deleteDir = new File(path).delete();
					if (!deleteDir){
						System.out.println("Could not delete " + path + " directory \n\tTry deleting the database first.\n");
					} else {
						System.exit(0);
					}
				} else {
					System.out.println ("Invalid option\n");
				}
			} catch (InputMismatchException e) {
				System.out.println("Invalid option\n");
			}
		}
	}
}