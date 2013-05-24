# Raiku

>Petals of the mountain rose

>Fall now and then,

>To the sound of the waterfall?


## Overview

Raiku is to Riak as a waterfall is to Akka; a simple Riak client which lets Riak flow to your Scala applications.

It's targeted as a non-blocking, performance focused, full-scale alternative to the java Riak driver.

Based on Akka IO and Sentinel, it uses the pipelines and actors to create the best throughput possible.

## Status

The client should be stable enough for day to day usage, but should still be treated as beta software.
Until version 1.0, the API of the client can and will change over versions, but shouldn't result in dramatic code changes.

**Currently available in the client:**

* Writing low-level protobuf style read-write objects through a RaikuClient;
* Doing this non-blocking through multiple connections handled by multiple actors (through Sentinel).
* Writing, fetching and deleting single or multiple objects at once;
* Querying items on 2i, based on binary or integral indexes (ranges also supported);
* Sequencing and continuing multiple operations using Tasks;
* Reactive Map/Reduce functionality;
* Auto-Reconnecting client;
* Different actor pools for *normal* and MR requests;
* Naive Reactive bucket for reactive data flows.

**The following is currently missing in the client, but will be added soon:**

* More solid, better reactive implementation;
* Additional documentation for specific use cases;
* Link walking;
* Search support.

## Architecture

The client uses Akka IO and pipelines to send and receive protocol buffer encoded data streams over TCP sockets.

Protocol Buffer messages are transformed into case classes using ScalaBuff, Riak PBC Content classes are serialized into `RaikuRawValue`, a type containing all information and metadata needed to write objects back into Riak.

You can use the client to fetch, store and delete these "low level" objects, but it's wiser to use the RaikuBucket to store objects converted using a RaikuConverter implementation.

You are free to use any value serialisation method available, but I recommended to use the Spray JSON package (behaves very good in multi-threaded environments).

Values returned from a `RaikuBucket` are of a `RaikuValue[T]` type. RaikuValue wraps the raw converted type into a *container* without losing any of the information present in the RaikuRawValue.
To retain the actual value of type `T` from the RaikuValue, a *value* property is available, containing the optional value of `T` (for cases where only the head was fetched from Riak).

In the package object of Raiku, a implicit function `unwrapRaikuValue` is available and combined with some `shapeless` magic, Raiku makes you able to use `RaikuValue[T]` as a normal `T` anywhere in your code (while throwing a exception when the value isn't available) and is able to handle both boxed as unboxed `T` values using the same functions.

All operations return a value in a Task. Task combines a `Try`, `Future` and `IO` Monad into one type: exceptions will be caught in the Try, all async actions are abstracted into a future monad and all IO actions are as pure as possible by using the Scalaz IO monad.

You can use `run` to expose the Future, or use `start(d: Duration)` to perform IO and wait (blocking) on the future.

For more information about the inner workings of Raiku, a blog post is available through the LAB050 site: [The anatomy of Raiku](http://lab050.com/blog/2013/3/6/the-anatomy-of-raiku-a-akka-based-riak-client) (will be updated soon with the newer sentinel based inner workings)

## Installation

You can use Raiku by source (by publishing it into your local Ivy repository):

<notextile><pre><code>./sbt publish-local
</code></pre></notextile>

Or by adding the repo:
<notextile><pre><code>"gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master"</code></pre></notextile>

to your SBT configuration and adding the `SNAPSHOT` to your library dependencies:

```scala
libraryDependencies ++= Seq(
	"nl.gideondk" %% "raiku" % "0.4.0"
)
````

### Play Framework 2.0
For usage in combination with Play2.0, you have to use a Play2.0 version compiled against Akka 2.2, until Akka 2.2 integration is pushed into mainstream, you can find a version at: [https://github.com/gideondk/Play2.0](https://github.com/gideondk/Play20).

When using the trunk version, remember to set:
<notextile><pre><code>addSbtPlugin("play" % "sbt-plugin" % "2.2-SNAPSHOT")</code></pre></notextile>

In your `plugins.sbt` to actually use the new Play version in your project.


## Usage
Using the client / bucket is quite simple, check the code of the tests to see all functionality. But it basically comes down to this:

**Create a client:**
```scala
implicit val system = ActorSystem("system")
val client = RaikuClient("localhost", 8087, 4)
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
val bucket = RaikuBucket[Y]("raiku_test_y_bucket", client)
```

## DSL
You can use the *normal* functions to store, fetch or delete objects:

<code>fetch</code> / <code>fetchMany</code>

<code>store</code> / <code>storeMany</code>

<code>delete</code> / <code>deleteMany</code>

Or to fetch keys on 2i:

<code>fetchKeysForBinIndexByValue</code>

<code>fetchKeysForIntIndexByValue</code>

<code>fetchKeysForIntIndexByValueRange</code>

If you like to take a walk on the wild side, you can try the (currently quite primitive) DSL to do these actions:

**Fetching objects**
```scala
persons ?   personId
persons ?* 	List(personIdA, personIdB)
```

**Storing objects**
<notextile><pre><code>persons <<   Person("Basho", 42, "Japan")
persons <<*  List(Person("Basho", 42, "Japan"), Person("Shiki", 52, "Japan"))
</code></pre></notextile>

**Deleting objects**
```scala
persons - 	Person("Basho", 42, "Japan")
persons -* 	 List(Person("Basho", 42, "Japan"), Person("Shiki", 52, "Japan"))
```

**Querying objects based on 2i**
```scala
persons idx  ("age", 42)
persons idx	 ("country", "Japan")
persons idx	 ("age", 39 to 50)
```

## MapReduce
Raiku features full MapReduce support through both a *Reactive API* as through normal `Futures`.

One important thing to note is that the MapReduce functionality is run through different actors / sockets then the normal functionality. MapReduce jobs therefor won't consume or block the normal flow of the client.

MapReduce can be used through a unique DSL, starting with the creation of input for the MapReduce job, for instance (all options can be found in source and tests):

`MR.bucket(bucketName: String)`: for a bucket based job

`MR.items(bucketAndKeys: Set[(String, String)])`: for a item based job

`MR.binIdx(bucketName: String, idx: String, idxv: String)`: for a index based job

### Phases
After creation of the specific input, several functions can be defined to be used as phases within the MapReduce job.

```scala
val a = BuildInMapFunction("Riak.mapValuesJson")
val b = BuildInMapFunction("Riak.filterNotFound")
val c = BuildInReduceFunction("Riak.reduceSum")
val d = BuildInReduceFunction("Riak.reduceMin")
```

The defined input can be used to be injected into the several phases:
```scala
val job = input |>> a >-> b >=> c >-> d
```

By Riak default, the last phase is automatically returned. Other phases can be returned by using the `>=>` between two phases (as opposed to the normal `>->`).

The run the job, the job is send to the client using the `mapReduce` function.

```scala
client mapReduce job
res0: Tuple2[List[JsValue], List[JsValue]]
```

This enables you to use the MapReduce results in a type safe manner (without run-time checking on the amount of phases). If you want to use the results from a MapReduce job in a Reactive manner, the `streamMapReduce` function can be send to the client:

```scala
client streamMapReduce job
res0: Tuple2[Enumerator[JsValue], Enumerator[JsValue]]
```

Returning a `Tuple` of `Play` powered broadcast `Enumerators`, streaming multiple results in a *map phase* and streaming a single result during a *reduce phase*.

## Reactive API
To expose further reactive functionality for usage in your favorite reactive web stack, the client is implementing a naive *Reactive API*.

Because Riak doens't support bulk inserts, bulk fetches or cursors, the Reactive API won't give you additional performance on top of the multi-fetch & multi-store `Future` implementation. But enables you to compose data flows in a more natural way, leveraging the awesome Reactive API `Play2.0` exposes.

Because of the nature of Riak and Iteratees, fetching isn't done in parallel, resulting in (possible) lower performance then the normal API but shouldn't consume additional resources as opposed to the normal functionality.

The `RaikuReactiveBucket` exposes the normal `fetch`, `store` and `delete` functionality to be used in combination with `Enumerators` instead of Lists of keys. `Iteratees` are added as end-points for a reactive data flow. `Enumeratees` are implemented to be used in more complex compositions:

```scala
Enumerator(randomObjects: _*) &>
bucket.storeEnumeratee(returnBody = true) &>
Enumeratee.filter(_.isDefined) &> Enumeratee.map(x ⇒ x.get) &> bucket.deleteEnumeratee() |>>>
Iteratee.fold(0) { (result, chunk) ⇒ result + 1 }
```

## (co)Monadic behavior
You can use the monadic behavior of <code>Task[RaikuValue[T]]</code> to combine multiple requests:

```scala
val objs: Task[List[RaikuValue[Person]]] = for {
	keys <- persons idx ("age", 39 to 50)
	objs <- persons ?* keys
} yield objs
```

Or better, using Scalaz:

```scala
persons idx ("age", 39 to 50) >>= ((x: List[String]) => bucket ?* x)
```

Or if you want to run queries in parrallel:

```scala
val storePersons = persons <<* perObjs
val storeCountries = countries <<* countryObjs
Task.sequenceSuccesses(List(storePersons, storeCountries))
```

Because `Task` is both a Monad as a Comonad, it's also possible to use (Scalaz powered) comonadic operations on Task:

```scala
val a: Task[Int] = 2.point[Task] 
val b: Int = a.copoint
```

## Credits

Lots of credits go to [Jordan West](https://github.com/jrwest) for the structure of his Scaliak driver,
and to [Sandro Gržičić](https://github.com/SandroGrzicic) for the ScalaBuff implementation.

## License
Copyright © 2013 Gideon de Kok

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
