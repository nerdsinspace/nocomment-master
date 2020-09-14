FROM adoptopenjdk/openjdk14
COPY . /code
WORKDIR /code
RUN ./gradlew shadowJar
CMD bash -c "while [ true ]; do java -Dchronicle.values.dumpCode=true -Xms10G -Xmx10G -XX:+UnlockExperimentalVMOptions -XX:+UseShenandoahGC -Xlog:gc+stats -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:-UseBiasedLocking -XX:+DisableExplicitGC -XX:MaxGCPauseMillis=450 -XX:+ExplicitGCInvokesConcurrent -XX:+ParallelRefProcEnabled -XX:+PerfDisableSharedMem -XX:+HeapDumpOnOutOfMemoryError -jar /code/build/libs/nocomment-master-1.0-SNAPSHOT-unoptimised.jar 2>&1 | tee -a /root/server.log; sleep 5; done"
