make btree:
	export CLASSPATH=$$CLASSPATH\:.\:/oracle/jdbc/lib/classes12.zip
	LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
	javac *.java
	java -cp .:/usr/share/java/db.jar mydbtest btree

make hash:
	export CLASSPATH=$$CLASSPATH\:.\:/oracle/jdbc/lib/classes12.zip
	LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
	javac *.java
	java -cp .:/usr/share/java/db.jar mydbtest hash

make indexfile:
	export CLASSPATH=$$CLASSPATH\:.\:/oracle/jdbc/lib/classes12.zip
	LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
	javac *.java
	java -cp .:/usr/share/java/db.jar mydbtest indexfile

