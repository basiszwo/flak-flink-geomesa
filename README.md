# Accumulo Setup

## About

This package is to be used to setup accumulo.

We setup a `SimpleFeatureType` named `FlakAcceleration` along with the folowing schema

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

This will create the Simple Feature Type defined in the `accumulot-setup` package.

```
java -cp flinkgeomesa-1.0-SNAPSHOT-jar-with-dependencies.jar \
  one.flak.accumulosetup.AccumuloSetup \
  --instanceId flakone \
  --zookeepers accumulo-1.flak.one:2181 \
  --user root \
  --password xxx \
  --tableName flak_samples
```
