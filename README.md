<p align="center">
    <a href="https://polypheny.org/">
        <picture><source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/polypheny/Admin/master/Logo/logo-white-text_cropped.png">
            <img width='50%' alt="Light: 'Resume application project app icon' Dark: 'Resume application project app icon'" src="https://raw.githubusercontent.com/polypheny/Admin/master/Logo/logo-transparent_cropped.png">
        </picture>
    </a>    
</p> 

# Polypheny JDBC Driver

This repository contains s JDBC driver for Polypheny. It utilizes the *Prism query interface* deployed with every instance of Polypheny by default. The driver adheres to the JDBC 4.2 standard, ensuring compatibility with Java applications, including those written in Scala and Kotlin, as well as tools like DataGrip.

This driver is compatible with JVM version 8 or higher.

## Getting Started

- The driver is published to Maven Central. Make sure that you have added `mavenCentral()` to the repositories section in your gradle build file.
- Add `implementation group: 'org.polypheny', name: 'polypheny-jdbc-driver', version: '3.0'` .
- Optionally: load the driver `org.polypheny.jdbc.PolyphenyDriver`, for example via
  ```
  Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
  ```
- Use the connection URL `jdbc:polypheny:http://localhost/` to connect to [Polypheny](https://github.com/polypheny/Polypheny-DB).

> For authentication, please use the default username `pa` with an empty password.

## Cypher and graph results

The graph support in this branch requires a Polypheny server with Prism graph
support. Use the driver's multi-model API to run Cypher and pass the name of an
existing graph namespace to `PolyStatement.execute`.

```java
try (PolyStatement statement = connection.unwrap(PolyConnection.class).createPolyStatement()) {
    GraphResult graph = statement.execute("social", "cypher", "MATCH (n:Person) RETURN n")
            .unwrap(GraphResult.class);
    for (PolyGraphElement element : graph) {
        System.out.println(element.getId());
    }

    RelationalResult rows = statement.execute("social", "cypher",
            "MATCH (n:Person) RETURN n AS person, n.name AS name").unwrap(RelationalResult.class);
    for (PolyRow row : rows) {
        PolyNode person = row.get("person").asNode();
        System.out.println(person.getId() + " " + row.get("name").asString());
    }
}
```

Queries that return only graph elements use `GraphResult`, an iterable stream of
nodes and edges. A path in that stream appears in traversal order as alternating
nodes and edges. Result pages are fetched during iteration and retained for
repeat iteration. Mixed projections require Prism 2.3 and preserve the row and
column structure. Read graph values as `PolyNode`, `PolyEdge`, `PolyPath` or
`PolyGraph`.

## Roadmap
See the [open issues](https://github.com/polypheny/Polypheny-DB/labels/A-jdbc) for a list of proposed features (and known issues).

## Contributing
We highly welcome your contributions to the _Polypheny JDBC Driver_. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project.


## License
The Apache 2.0 License
