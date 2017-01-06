# Raiku

> Petals of the mountain rose

> Fall now and then,

> To the sound of the waterfall?


## Overview

Raiku is to Riak as a waterfall is to Akka; a simple Riak client which lets Riak flow to your Scala applications.

It's targeted as a non-blocking, performance focused, full-scale alternative to the java Riak driver.

Based on Akka IO and Sentinel, it uses the pipelines and actors to create the best throughput possible.

## Status
The client should be stable enough for day to day usage, but should still be treated as beta software.

The next milestone releases of 0.8.0 will focus on the addition of CRDT and Map Reduce functionality, while trying to minimise API changes as good as possible. 

**Currently available in the client:**

* Writing low-level protobuf style read-write objects through a RaikuClient;
* Doing this non-blocking through multiple connections handled by multiple actors (through Sentinel).
* Writing, fetching and deleting single or multiple objects at once;
* Querying items on 2i, based on binary or integral indexes (ranges also supported);
* Pagination and streaming of 2i queries; 
* Auto-Reconnecting client;
* Different actor pools for *normal* and 2i requests;

**The following is currently missing in the client, but will be added in the upcoming 0.7.0 milestones:**

* Full CRDT support with Scala-like DSL for counters and collections;
* Redesigned Map Reduce functionality;
* Full Yokozuna support;
* Additional documentation for specific use cases.

## Roadmap
### 0.8.0 release
The 0.8.0 release will integrate Akka Streams as the architecture for both streaming 2i as MR results.  

### 1.0.0 release
This version will be the first stable 1.0 release, and will contain a revised test suite and an actor driven test kit, able to emulate Riak’s behaviour for integration within application tests. 

## Riak 1.4.1+
From version Raiku version 0.6.1 and on, only Riak version 1.4.1+ is tested and supported. 

It's possible that the client will work perfectly with older versions, but isn't tested and could result in unexpected behavior. 

Please use Raiku 0.5.1 for usage with older versions of Riak.

Upcoming functionality such as CRDTs and search are only supported on the (current in Beta) 2.0 release of Riak.

## Architecture

The client uses Akka IO and pipelines to send and receive protocol buffer encoded data streams over TCP sockets.

Protocol Buffer messages are transformed into case classes using ScalaBuff, Riak PBC Content classes are serialised into `RaikuRawValue`, a type containing all information and metadata needed to write objects back into Riak.

You can use the client to fetch, store and delete these "low level" objects (through respectively `fetchRaw`, `storeRaw` and `deleteRaw`), but for less custom use cases it's wiser to use the `RaikuBucket` to store objects converted using a `RaikuConverter` implementation.

You are free to use any value serialisation method available, but I recommended to use the Spray JSON package or the one available with the Play Framework

Next to the retrieval of `RaikuRawValue`, the `RaikuClient` implements functionality to store values of `RaikuValue[T]`. A `RaikuValue` wraps the raw converted type into a *container* without losing any of the information present in the `RaikuRawValue`. By using the `fetch`, `store` and `delete` functions on the client, the best approach can be taken to store serialisable types into buckets while maintaining complete control on all parameters and metadata. 

For easy usage, a `RaikuBucket[T]` is available, exposing the above functionality through a more opinionated and easy to use construct. While some parameters can still be set per bucket or per request through a custom `RaikuBucketConfig`. The API of the `RaikuBucket` is mostly focused on quick and easy usage of the functionality available in `Raiku` and should cover most use cases.  

Since Raiku is completely asynchronous, all methods return values wrapped in Futures.

## Installation
You can use Raiku by source (by publishing it into your local Ivy repository):

<notextile><pre><code>./sbt publish-local
</code></pre></notextile>

Or by adding the repo:
<notextile><pre><code>"gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master"</code></pre></notextile>

to your SBT configuration and adding Raiku to your library dependencies:

```scala
libraryDependencies ++= Seq(
  "nl.gideondk" %% "raiku" % "0.8-M1"
)
```

## Usage
Using the client / bucket is quite simple, check the code of the tests to see all functionality. But it basically comes down to this:

**Create a client:**
```scala
implicit val system = ActorSystem("system")
val client = RaikuClient("localhost", 8087)
```

**Create a converter:**
```scala
implicit val yFormat = jsonFormat4(Y)

implicit val yConverter = RaikuConverter.newConverter(
  reader = (v: RaikuRWValue) ⇒ yFormat.read(new String(v.data).asJson),
  writer = (o: Y) ⇒ RaikuRWValue(o.id, o.toJson.toString.getBytes, "application/json"),
  binIndexes = (o: Y) ⇒ Map("group_id" -> Set(o.groupId)),
  intIndexes = (o: Y) ⇒ Map("age" -> Set(o.age)))
```

**Finally, create the bucket:**
```scala
val bucket = RaikuBucket.default[Y](“raiku_test_y_bucket", client)
```

## DSL
On a `RaikuBucket`, you can use the *normal* functions to store, fetch or delete objects:

<code>fetch</code> / <code>fetchMany</code>

<code>store</code> / <code>storeMany</code>

<code>delete</code> / <code>deleteMany</code>

Or to fetch keys on 2i:

<code>fetchKeysForBinIndexByValue</code>

<code>fetchKeysForBinIndexByValueRange</code>

<code>fetchKeysForIntIndexByValue</code>

<code>fetchKeysForIntIndexByValueRange</code>

If you like to take a walk on the wild side, you can try the (currently quite primitive) DSL to do these actions:

**Fetching objects**
```scala
persons ?   personId
persons ?*  List(personIdA, personIdB)
```

**Storing objects**
<notextile><pre><code>persons <<   Person("Basho", 42, "Japan")
persons <<*  List(Person("Basho", 42, "Japan"), Person("Shiki", 52, "Japan"))
</code></pre></notextile>

**Deleting objects**
```scala
persons -   Person("Basho", 42, "Japan")
persons -*   List(Person("Basho", 42, "Japan"), Person("Shiki", 52, "Japan"))
```

**Querying objects based on 2i**
```scala
persons idx  ("age", 42)
persons idx  ("country", "Japan")
persons idx  ("age", 39 to 50)
persons idx  ("group_id", "A" to "B")
```

## 2i
Raiku supports the newer 2i functionality available in Riak 1.4.0 through pagination and result streaming.

### Pagination
When using secondary indexes, it's possible to set a maximum number of results for both integral as binary index queries (ranges also supported): 

```scala
bucket idx ("age", 20 to 50, maxResults = 20) 

bucket idx ("group_id", "A", 40)
```

Queries with a maximum in results not only return the normal `List[String]` containing the keys, but also returns a option on a *continuation*, combining the result to a `Future[(Option[String], List[String])]`.

This continuation value can be used to get the succeeding values of a specific query, making it able to paginate through values: 

```scala
for 
  (continuation, items) <- bucket idx ("age", 20, 10)
  (newContinuation, newItems) <- (bucket idx ("age", 20, maxResults = 10, continuation = continuation)
} yield newItems

```

Passing a `None` to the index query as the continuation value, treats the query as to paginate from start. When taking results through this pagination functionality, treat a `None` as returning continuation key as a *end-of-content*.

### Streaming
For each non-paging 2i query, a `streamIdx` equivalent is available. Instead of returning a `Future[List[String]]` for index values, keys are streamed back using a (Akka Streams) *Source*, wrapping each result into a `Future[Source]`.

This Play powered enumerator can directly be used to directly stream results to (web)clients, but can also be used to be composed into more complex pipelines.

## Credits
Lots of credits go to [Jordan West](https://github.com/jrwest) for the structure of his Scaliak driver,
and to [Sandro Gržičić](https://github.com/SandroGrzicic) for the ScalaBuff implementation.

## License
Copyright © 2017 Gideon de Kok

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
