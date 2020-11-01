FROM ballistacompute/spark:3.0.0
USER root
ADD benchmarks/build/distributions/benchmarks.tar /opt
RUN rm -f /opt/benchmarks/lib/scala-*.jar
RUN cp /opt/spark/jars/scala-*.jar /opt/benchmarks/lib/

#ENTRYPOINT [ "/opt/benchmarks/bin/benchmarks" ]