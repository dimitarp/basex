# BaseX Native Image

## Build

### Prerequisites

1. Install the latest Oracle GraalVM JDK11 and [Native Image Generator](https://www.graalvm.org/docs/reference-manual/native-image/), either the enterprise (EE) or the community edition (CE)

2. Install the latest Apache Maven

3. Setup Maven to use GraalVM as JDK, e.g.:

```bash
export JAVA_HOME=$GRAALVM_DIR
```

### Building the Binary

1. Build BaseX Core

Execute the following in the root Git directory:

```bash
mvn --projects basex-core \
    clean install \
    -Dmaven.test.skip=true
```

2. Build BaseX Native

Execute the following in the root Git directory:

```bash
mvn --projects basex-native \
    --activate-profiles native \
    clean package
```

The property `graalvm-ee` activates additional optimizations, which are only available when using GraalVM EE:

```bash
mvn --projects basex-native \
    --activate-profiles native \
    clean package \
    -Dgraalvm-ee
```

The binary executable will be generated in `basex-native/target`.

## Status

The following BaseX Core features are _not_ available in the native image:

| Feature | Description |
|---------|-------------|
| `xslt:transform`, `xslt:transform-text` | The Apache Xalan XSLT compiler in the JDK uses Translets and generates runtime classes using bytecode generator (BCEL). Currently, it is not possible to execute arbitrary bytecode using the Substrate VM. The Saxon XSLT Processor may be an option (TODO). |
| Support for XSD 1.1 with `validate:xsd` | Currently, the JDK's internal Xerces implementation supports only XSD 1.0. XSD 1.1 is supported by Saxon EE and Xerces builds with active XSD 1.1 support (e.g. `org.exist-db.thirdparty.xerces:xercesImpl`) |
| Lucene and Snowball Stemmers | TODO |
| Wordnet Stemmer | TODO |
| Igo Japanese Tokenizer | TODO |
| UCA Collations | TODO |
| SQL Module | Currently, it is not possible to execute arbitrary bytecode using the Substrate VM. So there is no way to specify a JDBC driver at runtime. |
| Java Modules | Currently, it is not possible to execute arbitrary bytecode using the Substrate VM. So there is no way to specify a Java module at runtime. |
| Java Bindings | Runtime reflection is currently not supported by the native image generator. |
| BaseX GUI | Currently, AWT is not supported by the native image generator. |
| ??? | |

## Comparison to Bytecode Binary

| | Bytecode | Native |
|-|:--------:|:------:|
| JVM required | yes | no |
| Platform independent | yes | no |
| Image Size | ~400KiB + ~2MiB Libs + JVM | ~74 MiB |
| Start time | ~400 ms | ~20 ms |
| Performance | faster | slower (up to 30%) |
| Memory | more (up to 80%) | less |

## TODOs

1. Windows build
2. Functional tests for missing features (e.g. using QT3)
