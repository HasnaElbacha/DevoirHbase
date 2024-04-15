FROM openjdk:8-jdk-alpine
COPY target/devoir-Hbase-1.0-SNAPSHOT-shaded.jar /app.jar
CMD ["java","-jar","/Main.jar"]