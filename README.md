# Accumulo Setup

## About

This package is to be used to setup accumulo.

We setup a `SimpleFeatureType` named `IothAccelerations` along with the folowing schema

```
"OccuredAt:Date"
"TripIdentifier:String"
"*SamplePosition:Point:srid=4326"
"AccelerationZ:Float"
```

## Usage

### Create jar

Running

```
mvn clean package
```

will create a fat jar with all required dependencies.


### Upload jar

```
scp target/accumulo-setup-1.0-SNAPSHOT-jar-with-dependencies.jar user@host:
```


### Run the setup program

```
java -cp ~/accumulo-setup-1.0-SNAPSHOT-jar-with-dependencies.jar \
    IothAccumuloSetup \
    -instanceId ioth \
    -zookeepers localhost:2181 \
    -user root \
    -password <rootPassword> \
    -tableName <namespace>.<tableName>
```

The `namespace` needs to be aligned with the one from the accumulo setup when installing the geomesa support to accumulo.
`tableName` can be anything. e.g. namespace is "geomesa" and tableName is "iothSamples" then the table
`geomesa.iothSamples` will be used (and created if it doesn't exist)
