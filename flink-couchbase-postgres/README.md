Note: Validated with JDK1.8

A Flink application project using Scala and SBT.

To run and test your application locally, you can just execute `sbt run` then select the main class that contains the Flink job . 

You can also package the application into a fat jar with `sbt assembly`, then submit it as usual, with something like: 

```
sbt assembly
flink run -c org.example.CouchbaseJob /path/to/your/project/my-app/target/scala-2.12/flinnk-assembly-0.1-SNAPSHOT.jar
```


You can also run your application from within IntelliJ:  select the classpath of the 'mainRunner' module in the run/debug configurations.
Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the "Use classpath of module" dropbox. 
