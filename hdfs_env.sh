#export HADOOP_HOME="/home/ubuntu/decio/hdfs/hadoop-dist/target/hadoop-3.3.9-SNAPSHOT/"
#export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
# Link native library
export LIBHDFS_DIR=$HADOOP_HOME/lib/native
export LD_LIBRARY_PATH=$LIBHDFS_DIR:$LD_LIBRARY_PATH
export CGO_LDFLAGS="-L$LIBHDFS_DIR -lhdfs"
export CGO_CFLAGS="-I$HADOOP_HOME/include"
# Set CLASSPATH
export CLASSPATH=$(hadoop classpath --glob)
# Set CGO flags
export CGO_LDFLAGS="-L$LIBHDFS_DIR -lhdfs"
export CGO_CFLAGS="-I$HADOOP_HOME/include"
# set for declaration executable
export DEC_EXEC="/users/yiweiche/dec-hdfs/hdfs-declarative-io/hadoop-hdfs-project/hadoop-hdfs-native-client/target/main/native/libhdfspp/examples/cc/declarative-io"
