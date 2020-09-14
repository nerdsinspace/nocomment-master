FROM adoptopenjdk/openjdk14:latest 

ADD build/libs/nocomment-master*.jar /opt/nocom-master/master.jar
WORKDIR /opt/nocom-master

ENV JVM_ARGS=''

ENTRYPOINT java ${JVM_ARGS} -jar master.jar
