FROM scratch as scratch

COPY . /code/

FROM openzipkin/zipkin-builder as built

COPY --from=scratch /code /code

WORKDIR /code

RUN ./gradlew build -x check

WORKDIR /zipkin-berkeley

RUN cp /code/build/libs/zipkin-berkeleydb*.jar berkeley.jar && \
  (mkdir berkeley && cd berkeley && jar -xf ../berkeley.jar) && \
  rm berkeley.jar

FROM openzipkin/zipkin:master
MAINTAINER Jon Schneider

COPY --from=built /zipkin-berkeley/ /zipkin/

# Readback is currently not supported
ENV QUERY_ENABLED false

ENV MODULE_OPTS="-Dloader.path=berkeley -Dspring.profiles.active=berkeley"
