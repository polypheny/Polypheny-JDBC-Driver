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
- Add `implementation group: 'org.polypheny', name: 'polypheny-jdbc-driver', version: '3.2'` .
- Optionally: load the driver `org.polypheny.jdbc.PolyphenyDriver`, for example via
  ```
  Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
  ```
- Use the connection URL `jdbc:polypheny:http://localhost/` to connect to [Polypheny](https://github.com/polypheny/Polypheny-DB).

> For authentication, please use the default username `pa` with an empty password.

## Server errors

Starting with driver 3.3, JDBC operations expose the SQLSTATE and vendor error code from final Prism error responses via `SQLException.getSQLState()` and `getErrorCode()`. Structured error details are available starting with Prism 2.4. A missing or invalid SQLSTATE falls back to `UNSPECIFIED`. A missing error code falls back to `-1`, and an explicitly supplied `0` is preserved. `PrismInterfaceServiceException` remains the driver exception for server errors, with the original server message available through `getMessage()`. A non-final error response has the same fallback values and a `ProtocolException` cause.

## Roadmap
See the [open issues](https://github.com/polypheny/Polypheny-DB/labels/A-jdbc) for a list of proposed features (and known issues).

## Contributing
We highly welcome your contributions to the _Polypheny JDBC Driver_. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project.


## License
The Apache 2.0 License
