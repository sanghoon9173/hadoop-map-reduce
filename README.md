Useful commands:

export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

# compile/build
hadoop com.sun.tools.javac.Main <\name-of-the-file>.java -d build

# create jar
jar -cvf <\name-of-the-jar>.jar -C build/ ./

#force remove the ouput to re-run the map reduce
hadoop fs -rm -r <\output-dir>

# run map reduce saving the output to DFS
hadoop jar <\name-of-the-jar>.jar <\name-of-the-jar> <\input-dir> <\output-dir>

# read output from DFS
hadoop fs -cat <\ouput-dir>/part*

