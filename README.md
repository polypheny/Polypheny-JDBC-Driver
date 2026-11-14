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

## Document collection metadata

Document metadata is available through the existing Prism client exposed by `PolyConnection`:

```java
try (java.sql.Connection connection = java.sql.DriverManager.getConnection(
        "jdbc:polypheny://localhost:20590", "pa", "")) {
    org.polypheny.jdbc.PolyConnection polyConnection =
            connection.unwrap(org.polypheny.jdbc.PolyConnection.class);
    polyConnection.getPrismInterfaceClient()
            .searchEntities("patients_symptoms", "sym%", connection.getNetworkTimeout())
            .stream().filter(entity -> entity.hasDocument())
            .forEach(entity -> {
                System.out.println(entity.getDocument().getCollectionName());
                entity.getDocument().getFieldsList().forEach(field ->
                        System.out.println(field.getFieldPath() + ": " + field.getTypeName()));
            });
}
```

The document message includes the namespace and collection names, known fields with type and optionality,
an optional JSON sample (`hasSampleDocumentJson()`), and document indexes with field path, method and uniqueness.
Field paths separate nested keys with `.` and escape literal dots and backslashes inside keys with `\`.
Inferred fields describe the server's sample rather than an enforced schema; unavailable samples and indexes
remain absent or empty. A unique index flag applies to single-field indexes and their indexed documents.
The standard JDBC table and column metadata methods continue to describe relational entities.

The driver uses the published Prism 2.2 schemas and requires a compatible Prism 2.x server.
The server must implement document metadata to populate these fields. Older compatible
servers can still return the original empty `Document` message.

## Roadmap
See the [open issues](https://github.com/polypheny/Polypheny-DB/labels/A-jdbc) for a list of proposed features (and known issues).

## Contributing
We highly welcome your contributions to the _Polypheny JDBC Driver_. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project.


## License
The Apache 2.0 License
