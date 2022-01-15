<a href="https://polypheny.org/">
    <img align="right" width="200" height="200" src="https://polypheny.org/community/logo/logo.png" alt="Resume application project app icon">
</a>

# Polypheny JDBC Driver

This repository contains a standards-compliant JDBC driver for Polypheny-DB.

- Load the driver `org.polypheny.jdbc.Driver`, for example via 
  ```
  Class.forName( "org.polypheny.jdbc.Driver" );
  ```
- Use the connection URL `jdbc:polypheny:http://localhost/` to connect to [Polypheny-DB](https://github.com/polypheny/Polypheny-DB).

For authentification please use the default username `pa` with an empty password.


## Roadmap ##
See the [open issues](https://github.com/polypheny/Polypheny-DB/labels/A-jdbc) for a list of proposed features (and known issues).


## Contributing ##
We highly welcome your contributions to the _Polypheny JDBC Driver_. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project. 


## Credits ##
This JDBC Driver is based on [Apache Avatica](https://calcite.apache.org/avatica/), a framework for building database drivers. 


## License ##
The Apache 2.0 License
