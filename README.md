<p align="center">
    <a href="https://polypheny.org/">
        <picture><source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/polypheny/Admin/master/Logo/logo-white-text_cropped.png">
            <img width='50%' alt="Light: 'Resume application project app icon' Dark: 'Resume application project app icon'" src="https://raw.githubusercontent.com/polypheny/Admin/master/Logo/logo-transparent_cropped.png">
        </picture>
    </a>    
</p> 

# Polypheny JDBC Driver

This repository contains a standards-compliant JDBC driver for Polypheny.

## Getting Started

- The driver is published to Maven Central. Make sure that you have added `mavenCentral()` to the repositories section in your gradle build file.
- Add `implementation group: 'org.polypheny', name: 'polypheny-jdbc-driver', version: '1.5.3'` .
- Load the driver `org.polypheny.jdbc.Driver`, for example via
  ```
  Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
  ```
- Use the connection URL `jdbc:polypheny:http://localhost/` to connect to [Polypheny-DB](https://github.com/polypheny/Polypheny-DB).

> For authentication, please use the default username `pa` with an empty password.

## Roadmap
See the [open issues](https://github.com/polypheny/Polypheny-DB/labels/A-jdbc) for a list of proposed features (and known issues).

## Contributing
We highly welcome your contributions to the _Polypheny JDBC Driver_. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project.


## License
The Apache 2.0 License
