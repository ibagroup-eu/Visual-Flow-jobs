#
# Copyright (c) 2021 IBA Group, a.s. All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM ghcr.io/ibagomel/spark:spark-3.4.1-bin-hadoop3

ARG uid=1000
ARG gid=1000
ARG group=job-group
ARG username=job-user

USER root


RUN apt-get update \
    && apt-get install ca-certificates \
    && apt-get --only-upgrade install ca-certificates \
    && /usr/sbin/groupadd -g ${gid} ${group} \
    && /usr/sbin/useradd -u ${uid} -g ${gid} -m ${username}

COPY log4j.properties /opt/spark/conf/
COPY log4j2.xml /opt/spark/conf/
COPY target/spark-transformations-0.1-jar-with-dependencies.jar /opt/spark/work-dir/
COPY entrypoint.sh /opt/spark/work-dir/
RUN chmod -R 755 /opt/spark/work-dir
ADD https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/3.3.1_0.18.7/spark-excel_2.12-3.3.1_0.18.7.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.4.1/commons-pool2-2.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
RUN chmod -R 755 /opt/spark/jars
ADD https://s3.amazonaws.com/redshift-downloads/redshift-keytool.jar /opt/spark/work-dir/
RUN java -jar /opt/spark/work-dir/redshift-keytool.jar -s

USER ${username}

CMD ["/usr/bin/bash", "/opt/spark/work-dir/entrypoint.sh"]
