Unique ID generator
===================

A unique ID generator that generates unique¹ eight byte identifiers in a distributed context.

1. Unique within the confines of the chosen computing environment.

## What is it for?

When you want to assign unique identifiers to database records in a distributed computing
environment that are short and guaranteed to be unique (within your data realm), this
library can provide them.

## Versions

For Java 8 and newer, please use the `2.x` series of releases. For Java 7 version `1.14` may be
used.

## Local usage

If you have a computing environment where you know exactly which processes may generate IDs.
The simple `LocalUniqueIDGenerator` can be used. This generator assumes that you know which
process may use which generator-ID at any time.

For example, if you have just one process that handles the creation of new IDs (perhaps a
single server that creates database records using these IDs), a generator can be used like
this:

```java
final int generatorID = 0;
final int clusterID = 0;
IDGenerator generator = LocalUniqueIDGeneratorFactory.generatorFor(generatorID, clusterID);

// Generate IDs
byte[] id = generator.generate();
```

The `LocalUniqueIDGeneratorFactory` assumes that you can guarantee that it is the only 
generator with the specific generator-ID and cluster-ID combination you chose, during its 
lifetime.

If there is a fixed number of processes that may generate IDs, you can assign one of the
256 possible generator-IDs to each one. For a more in-depth explanation of generator-IDs
and cluster-IDs, see [eight byte ID structure](doc/eight-byte-id-structure.md).

For a cluster of Tomcat servers in a high-availability setup, you could configure a system 
property on each server with a unique generator ID.

For local usage the `uniqueid-core` module can be used:

```xml
<dependency>
  <groupId>org.lable.oss.uniqueid</groupId>
  <artifactId>uniqueid-core</artifactId>
  <version>${uniqueid.version}</version>
</dependency>
```

## Distributed usage with a ZooKeeper quorum

If you need to generate unique IDs within a distributed environment, automatic coordination of
the generator-ID is also a possibility. The acquisition of a generator-ID can be handled by a 
`SynchronizedGeneratorIdentity` instance, which uses
[Apache ZooKeeper](http://zookeeper.apache.org/) to claim its generator-ID — for a short while,
or as long as it maintains a connection to the ZooKeeper quorum. 

For this functionality the `uniqueid-zookeeper` module is used:

```xml
<dependency>
  <groupId>org.lable.oss.uniqueid</groupId>
  <artifactId>uniqueid-core</artifactId>
  <version>${uniqueid.version}</version>
</dependency>
```

### Preparing the ZooKeeper quorum

To use this method of generator-ID acquisition, a node on the ZooKeeper quorum must
be chosen to hold the queue and resource pool used by `SynchronizedGeneratorIdentity`.

For example, if you choose `/unique-id-generator` as the node, these child nodes will be
created:

```
/unique-id-generator/
 ├─ queue/
 ├─ pool/
 └─ cluster-id
```

Note that if you do not create the `cluster-id` node yourself (recommended), the default 
value of `0` will be used. To use a different cluster ID, set the content of this znode to 
one of the 16 permissible values (i.e., `0..15`).

If you have access to the `zkcli` (or `hbase zkcli`) command line utility you can set the 
cluster-ID like so:

```
create /unique-id-generator/cluster-id 1
```

Or if the node already exists:

```
set /unique-id-generator/cluster-id 1
```

### Using the generator

To use an `IDGenerator` with a negotiated generator-Id, create a new instance like this:

```java
// Change the values of zookeeperQuorum and znode as needed:
final String zookeeperQuorum = "zookeeper1,zookeeper2,zookeeper3";
final String znode = "/unique-id-generator";
IDGenerator generator = SynchronizedUniqueIDGeneratorFactory.generatorFor(zookeeperQuorum, znode);
// ...
byte[] id = generator.generate()
// ...
```

If you expect that you will be using dozens of IDs in a single process, it is more
efficient to generate IDs in batches:

```java
Deque<byte[]> ids = generator.batch(500);
// ...
byte[] id = ids.pop();
// etc.
```

If you intend to generate more than a few IDs at a time, you can also wrap the generator in
an `AutoRefillStack`, and simply call `generate()` on that whenever you need a new ID.
It will grab IDs in batches from the wrapped `IDGenerator` instance for you. This is
probably the simplest and safest way to use an `IDGenerator`.

```java
final String zookeeperQuorum = "zookeeper1,zookeeper2,zookeeper3";
final String znode = "/unique-id-generator";
IDGenerator generator = new AutoRefillStack(
    SynchronizedUniqueIDGeneratorFactory.generatorFor(zookeeperQuorum, znode)
);
// ...
byte[] id = generator.generate()
// ...
```
