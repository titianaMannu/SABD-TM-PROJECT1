# Creates pseudo distributed hadoop 3.1.4
#
# docker build -t effeerre/hadoop .

FROM debian
USER root

# install dev tools
RUN apt update && \
    apt install -y tar openssh-server openssh-client openjdk-11-jre-headless bash wget

# # java
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# # hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.1.4/hadoop-3.1.4.tar.gz -O /tmp/hadoop-3.1.4.tar.gz && \
	tar -zxf /tmp/hadoop-3.1.4.tar.gz -C /usr/local/ && \
	rm /tmp/hadoop-3.1.4.tar.gz
RUN ln -s /usr/local/hadoop-3.1.4 /usr/local/hadoop


ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
ENV HDFS_NAMENODE_USER "root"
ENV HDFS_DATANODE_USER "root"
ENV HDFS_SECONDARYNAMENODE_USER "root"
ENV YARN_RESOURCEMANAGER_USER "root"
ENV YARN_NODEMANAGER_USER "root"

# # pseudo distributed
ADD config/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh
ADD config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
#RUN chown hdfs:hdfs -R $HADOOP_HOME
#RUN chown hdfs:hdfs -R /usr/local/hadoop-3.1.4
# 
#ADD config/ssh_config /home/hdfs/.ssh/config
#RUN chmod 600 /home/hdfs/.ssh/config
#RUN chown hdfs:hdfs /home/hdfs/.ssh/config
# 
ADD bootstrap.sh /usr/local/bootstrap.sh
#RUN chown hdfs:hdfs /usr/local/bootstrap.sh
RUN chmod 755 /usr/local/bootstrap.sh
# 
ENV BOOTSTRAP /usr/local/bootstrap.sh
# 


# passwordless ssh
RUN mkdir -p /root/.ssh
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
ADD config/ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
#RUN chown hdfs:hdfs /home/hdfs/.ssh/config

#CMD ["/bin/bash"]
CMD /usr/local/bootstrap.sh

# Hdfs ports
EXPOSE 9866 9867 9870 9864 9868 9820 9000 54310
# SSH
EXPOSE 22
