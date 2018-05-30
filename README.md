Unique ID generator
===================

A unique ID generator that generates unique¹ eight byte identifiers in a distributed context.

1. Unique within the confines of the chosen computing environment.

## What is it for?

When you want to assign unique identifiers to objects (e.g., database records) in a
distributed computing environment that are short and guaranteed to be unique (within your
data realm), this library can provide them.

## Versions

For Java 8 and newer, please use the `2.x` series of releases. For Java 7 version `1.x` may be
used, but this version is no longer actively maintained.

## Concepts

To have multiple concurrent processes generate short unique identifiers, some form of
coordination is required. The two basic premises of this library are:

1. Each process that generates identifiers must claim or be assigned a number representing
   its **generator-ID** and incorporate that in the identifiers it generates
2. Each process that generates identifiers must have its clock synchronised and must
   incorporate the current **timestamp** in the identifiers it generates

Processes can generate up to 64 identifiers per millisecond; a **sequence** counter
is incorporated in each identifier that represents these 64 possibilities.

In addition to the generator-ID, the **cluster-ID** allows for 16 'clusters' of generators to
simultaneously generate identifiers. This is useful if several groups of processes have to
operate without being able to coordinate with each other in real time. Examples include
processes run on data clusters in separate data centres, and maintenance processes that
operate on off-line copies of data sets.

The identifiers generated are composed from the components mentioned above, and have
the following structure:

```
// Each letter represents one bit in the eight byte identifier.
TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTSSSSSS ...MGGGG GGGGCCCC
```

 * `T`: Timestamp (in milliseconds, bit order depends on mode)
 * `S`: Sequence counter
 * `.`: *Reserved for future use*
 * `M`: Mode
 * `G`: Generator-ID
 * `C`: Cluster-ID

See also: [eight byte ID structure](doc/eight-byte-id-structure.md).

### Modes

The **mode** flag is used to distinguish between the two modes of generating IDs: `SPREAD`
and `TIME_SEQUENTIAL`.

In `SPREAD` mode the IDs generated are meant to be used as opaque identifiers. Generated
IDs do not sort sequentially, but are instead 'spread out' over the eight byte address space.
This is useful when these IDs are used as part of a row-key in a key-value store, because the
non-sequential nature of the IDs can help prevent hot-spotting.

Conversely, the `TIME_SEQUENTIAL` mode is meant for IDs that *should* sort in order of their
time of creation, and can be useful in assigning time-based identifiers to objects, and prevent
objects created in the same millisecond instant from overlapping.

### Coordination

As long as a single identifier generating process has a claim on a unique
cluster-ID/generator-ID pair, it can generate unique identifiers (within the system it is a
part of). The cluster-ID is always assigned manually (the assumption being that only a very
limited number of 'clusters' exists). The generator-ID can also be assigned manually, but
usually it is desirable for processes to be able to exclusively claim a generator-ID
automatically.

This library facilitates this using [ZooKeeper](http://zookeeper.apache.org/).
Generators can stake a claim on a generator-ID for a short period of time (usually ten
minutes), and repeat this whenever IDs are generated.

## Usage

### Local usage

If you have a computing environment where you know exactly which processes may generate IDs.
The simple `LocalUniqueIDGenerator` can be used. This generator assumes that you know which
process may use which generator-ID at any time.

For example, if you have just one process that handles the creation of new IDs (perhaps a
single server that creates database records using these IDs), a generator can be used like
this:

```java
final int generatorID = 0;
final int clusterID = 0;
IDGenerator generator = LocalUniqueIDGeneratorFactory.generatorFor(generatorID, clusterID, Mode.SPREAD);

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
property on each server with a unique generator-ID, although this approach does assume that
there is only one ID generating instance running on that server.

For local usage the `uniqueid-core` module can be used:

```xml
<dependency>
  <groupId>org.lable.oss.uniqueid</groupId>
  <artifactId>uniqueid-core</artifactId>
  <version>${uniqueid.version}</version>
</dependency>
```

### Distributed usage with a ZooKeeper quorum

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

#### Preparing the ZooKeeper quorum

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

#### Using the generator

To use an `IDGenerator` with a negotiated generator-Id, create a new instance like this:

```java
// Change the values of zookeeperQuorum and znode as needed:
final String zookeeperQuorum = "zookeeper1,zookeeper2,zookeeper3";
final String znode = "/unique-id-generator";
IDGenerator generator = SynchronizedUniqueIDGeneratorFactory.generatorFor(zookeeperQuorum, znode, Mode.SPREAD);
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
probably the simplest and safest way to use an `IDGenerator` in the default `SPREAD` mode.

```java
final String zookeeperQuorum = "zookeeper1,zookeeper2,zookeeper3";
final String znode = "/unique-id-generator";
IDGenerator generator = new AutoRefillStack(
    SynchronizedUniqueIDGeneratorFactory.generatorFor(zookeeperQuorum, znode, Mode.SPREAD)
);
// ...
byte[] id = generator.generate()
// ...
```

For the `TIME_SEQUENTIAL` mode the above is usually not what you want, if you intend to use
the timestamp stored in the generated ID as part of your data model (the batched pre-generated
IDs might have a timestamp that lies further in the past then you might want).

```java
final String zookeeperQuorum = "zookeeper1,zookeeper2,zookeeper3";
final String znode = "/unique-id-generator";
IDGenerator SynchronizedUniqueIDGeneratorFactory.generatorFor(zookeeperQuorum, znode, Mode.TIME_SEQUENTIAL);
// ...
byte[] id = generator.generate()
// Extract the timestamp in the ID.
long createdAt = IDBuilder.parseTimestamp(id);
```
