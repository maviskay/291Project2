btree:
	export CLASSPATH=$$CLASSPATH\:.\:/oracle/jdbc/lib/classes12.zip
	export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
	javac *.java
	java -cp .:/usr/share/java/db.jar mydbtest btree

hash:
	export CLASSPATH=$$CLASSPATH\:.\:/oracle/jdbc/lib/classes12.zip
	export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
	javac *.java
	java -cp .:/usr/share/java/db.jar mydbtest hash

indexfile:
	export CLASSPATH=$$CLASSPATH\:.\:/oracle/jdbc/lib/classes12.zip
	export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
	javac *.java
	java -cp .:/usr/share/java/db.jar mydbtest indexfile

