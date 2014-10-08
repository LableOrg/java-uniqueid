Unique ID generator
===================

A small collection of unique ID generators, currently limited to eight byte key generating
generators.

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
IDGenerator generator = LocalUniqueIDGenerator.generatorFor(generatorID, clusterID);

// Generate IDs
byte[] id = generator.generate();
```

The `LocalUniqueIDGenerator` assumes that you can guarantee that it is the only generator
with that specific generator-ID and cluster-ID combination, during its lifetime.

If there is a fixed number of processes that may generate IDs, you can assign one of the
64 possible generator-IDs to each one. For a more in-depth explanation of generator-IDs
and cluster-IDs, see [eight byte ID structure](doc/eight-byte-id-structure.md).

## Distributed usage with a ZooKeeper quorum

If you need to generate unique IDs in a distributed environment, automatic coordination of
the generator-ID can be handled by the `SynchronizedUniqueIDGenerator` class, which uses
[Apache ZooKeeper](http://zookeeper.apache.org/) to synchronize access to a pool of
generator IDs.

### Preparing the ZooKeeper quorum

To use this method of resource pool synchronization, a ZNode on the ZooKeeper quorum must
be chosen to hold the queue and resource pool used by `SynchronizedUniqueIDGenerator`.

For example, if you choose `/unique-id-generator` as the ZNode, these ZNodes will be
created:

```
/unique-id-generator/
 ├─ queue/
 ├─ pool/
 └─ cluster-id
```

Note that if you do not create the `cluster-id` znode yourself, the default value of `0`
will be used. To use a different cluster ID, set the content of this znode to one of the
16 permissible values (`0..15`).

If you are using `zkcli`:

```
create /unique-id-generator/cluster-id 1
```
Or if the znode already exists:
```
set /unique-id-generator/cluster-id 1
```

### Using the generator

To use the `SynchronizedUniqueIDGenerator` create a new instance like this:

```java
// Change the values of zookeeperQuorum and znode as needed:
final String zookeeperQuorum = "zookeeper1,zookeeper2,zookeeper3";
final String znode = "/unique-id-generator";
IDGenerator generator = SynchronizedUniqueIDGenerator.generator(zookeeperQuorum, znode);
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
    SynchronizedUniqueIDGenerator.generator(zookeeperQuorum, znode)
);
// ...
byte[] id = generator.generate()
// ...
```
