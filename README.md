# Raiku

>Petals of the mountain rose

>Fall now and then,

>To the sound of the waterfall?


## Overview

Raiku is to Riak as a waterfall is to Akka; a simple Riak client which lets Riak flow to your Scala applications.

It's targeted as a non-blocking, performance focused, full-scale alternative to the java Riak driver.

Based on Akka IO, it uses the iteratee pattern and actors to create the best throughput possible.

## Status

The client should currently treated as a proof of concept, but is stable enough to try out in smaller projects (although the client is used in production in serveral applications).

**Currently available in the client:**

* Writing low-level protobuf style read-write objects through a RaikuClient;
* Doing this non-blocking through multiple actors, handled by a single supervisor;
* Writing, fetching and deleting single or multiple objects at once;
* Querying items on 2i, based on binary or integral indexes (ranges also supported);
* Sequencing and continuing multiple operations using monad transformers (ValidatedFuture, ValidatedFutureIO);
* Reactive Map/Reduce functionality;
* Auto-Reconnecting client with retrier functionality;
* Different actor pools for *normal* and MR requests;
* Naive Reactive bucket for reactive data flows.

**The following is currently missing in the client, but will be added soon:**

* Link walking;
* Custom mutations / conflict resolutions;
* Durable mailboxes;
* Least-connection-error-based router / pool;

## Architecture

The client uses Akka IO and iteratees to send and receive protocol buffer encoded data streams over TCP sockets.

Protocol Buffer messages are transformed into case classes using ScalaBuff, Riak PBC Content classes are serialized into *RWObjects*, which are case classes, containing all information needed to write objects back into Riak.

You can use the client to fetch, store and delete these "low level" objects, but it's wiser to use the RaikuBucket to store objects converted using a RaikuConverter implementation.

You are free to use any value serialisation method available, but I recommended to use the Spray JSON package (behaves very good in multi-threaded environments).

All operations return a value in a monad transformer (<code>ValidatedFutureIO</code>) which combines a <code>Validation</code>, <code>Future</code> and <code>IO</code> monad into one type: most (if not all) exceptions will be caught in the validation monad, all async actions are abstracted into a future monad and all IO actions are as pure as possible by using the Scalaz IO monad.

Use <code>unsafePerformIO</code> to expose the Future, or use <code>unsafeFulFill(d: Duration)</code> to perform IO and wait (blocking) on the future.

For more information about the inner workings of Raiku, a blog post is available through the LAB050 site: [The anatomy of Raiku](http://lab050.com/blog/2013/3/6/the-anatomy-of-raiku-a-akka-based-riak-client)

## Installation

You can use Raiku by source (by publishing it into your local Ivy repository):

<notextile><pre><code>./sbt publish-local
</code></pre></notextile>

Or by adding the repo:
<notextile><pre><code>"gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master"</code></pre></notextile>

to your SBT configuration and adding the `SNAPSHOT` to your library dependencies:

<notextile><pre><code>libraryDependencies ++= Seq(
	"nl.gideondk" %% "raiku" % "0.3.5-SNAPSHOT"
)
</code></pre></notextile>

### Play Framework 2.0
For usage in combination with Play2.0, you have to use a Play2.0 version compiled against Akka 2.2, until Akka 2.2 integration is pushed into mainstream, you can find a version at: [https://github.com/gideondk/Play2.0](https://github.com/gideondk/Play20).

When using the trunk version, remember to set:
<notextile><pre><code>addSbtPlugin("play" % "sbt-plugin" % "2.2-SNAPSHOT")</code></pre></notextile>

In your `plugins.sbt` to actually use the new Play version in your project.


## Usage
Using the client / bucket is quite simple, check the code of the tests to see all functionality. But it basically comes down to this:

**Create a client:**
<notextile><pre><code>implicit val system = ActorSystem("system")
val client = RaikuClient("localhost", 8087, 4)
</code></pre></notextile>

**Create a converter:**
<pre><code>implicit val yFormat = jsonFormat4(Y)

implicit val yConverter = new RaikuConverter[Y] {
	def read(o: RaikuRWObject): ReadResult[Y] = try {
		yFormat.read(new String(o.value).asJson).success
	} catch {
		case e : Throwable => e.failure
	}
	def write(bucket: String, o: Y): RaikuRWObject = RaikuRWObject(bucket, o.id, o.toJson.toString.getBytes, binIndexes = Map("group_id" -> List(o.groupId)), intIndexes = Map("age" -> List(o.age)))
}
</code></pre>

**Finally, create the bucket:**
<pre><code>val bucket = RaikuBucket[Y]("raiku_test_y_bucket", client)
</code></pre>

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
<pre><code>persons ?   personId
persons ?* 	List(personIdA, personIdB)
</code></pre>

**Storing objects**
<notextile><pre><code>persons <<   Person("Basho", 42, "Japan")
persons <<*  List(Person("Basho", 42, "Japan"), Person("Shiki", 52, "Japan"))
</code></pre></notextile>

**Deleting objects**
<pre><code>persons - 	Person("Basho", 42, "Japan")
persons -* 	 List(Person("Basho", 42, "Japan"), Person("Shiki", 52, "Japan"))
</code></pre>

**Querying objects based on 2i**
<pre><code>persons idx 	("age", 42)
persons idx	 ("country", "Japan")
persons idx	 ("age", 39 to 50)
</code></pre>

## MapReduce
Raiku features full MapReduce support through both a *Reactive API* as through normal `Futures`.

One important thing to note is that the MapReduce functionality is run through different actors / sockets then the normal functionality. MapReduce jobs therefor won't consume or block the normal flow of the client.

MapReduce can be used through a unique DSL, starting with the creation of input for the MapReduce job, for instance (all options can be found in source and tests):

`MR.bucket(bucketName: String)`: for a bucket based job

`MR.items(bucketAndKeys: Set[(String, String)])`: for a item based job

`MR.binIdx(bucketName: String, idx: String, idxv: String)`: for a index based job

### Phases
After creation of the specific input, several functions can be defined to be used as phases within the MapReduce job.

<notextile><pre><code>val a = BuildInMapFunction("Riak.mapValuesJson")
val b = BuildInMapFunction("Riak.filterNotFound")
val c = BuildInReduceFunction("Riak.reduceSum")
val d = BuildInReduceFunction("Riak.reduceMin")
</code></pre></notextile>

The defined input can be used to be injected into the several phases:
<notextile><pre><code>val job = input |>> a >-> b >=> c >-> d
</code></pre></notextile>

By Riak default, the last phase is automatically returned. Other phases can be returned by using the `>=>` between two phases (as opposed to the normal `>->`).

The run the job, the job is send to the client using the `mapReduce` function.

<notextile><pre><code>client mapReduce job
res0: Tuple2[List[JsValue], List[JsValue]]
</code></pre></notextile>

This enables you to use the MapReduce results in a type safe manner (without run-time checking on the amount of phases). If you want to use the results from a MapReduce job in a Reactive manner, the `streamMapReduce` function can be send to the client:

<notextile><pre><code>client streamMapReduce job
res0: Tuple2[Enumerator[JsValue], Enumerator[JsValue]]
</code></pre></notextile>

Returning a `Tuple` of `Play` powered broadcast `Enumerators`, streaming multiple results in a *map phase* and streaming a single result during a *reduce phase*.

## Reactive API
To expose further reactive functionality for usage in your favorite reactive web stack, the client is implementing a naive *Reactive API*.

Because Riak doens't support bulk inserts, bulk fetches or cursors, the Reactive API won't give you additional performance on top of the multi-fetch & multi-store `Future` implementation. But enables you to compose data flows in a more natural way, leveraging the awesome Reactive API `Play2.0` exposes.

Because of the nature of Riak and Iteratees, fetching isn't done in parallel, resulting in (possible) lower performance then the normal API but shouldn't consume additional resources as opposed to the normal functionality.

The `RaikuReactiveBucket` exposes the normal `fetch`, `store` and `delete` functionality to be used in combination with `Enumerators` instead of Lists of keys. `Iteratees` are added as end-points for a reactive data flow. `Enumeratees` are implemented to be used in more complex compositions:

<notextile><pre><code>Enumerator(randomObjects: _*) &>
bucket.storeEnumeratee(returnBody = true) &>
Enumeratee.filter(_.isDefined) &> Enumeratee.map(x ⇒ x.get) &> bucket.deleteEnumeratee() |>>>
Iteratee.fold(0) { (result, chunk) ⇒ result + 1 }
</code></pre></notextile>

## Monadic behavior
You can use the monadic behavior of <code>ValidatedFutureIO[T]</code> to combine multiple requests:

<notextile><pre><code>val objs: ValidatedFutureIO[List[Person]] = for {
	keys <- persons idx ("age", 39 to 50)
	objs <- persons ?* keys
} yield objs
</code></pre></notextile>

Or better, using Scalaz:

<pre><code>persons idx ("age", 39 to 50) >>= ((x: List[String]) => bucket ?* x)</code></pre>

Or if you want to run queries in parrallel:

<notextile><pre><code>val storePersons = persons <<* perObjs
val storeCountries = countries <<* countryObjs
ValidatedFutureIO.sequence(List(storePersons, storeCountries))</code></pre></notextile>


## Credits

Lots of credits go to [Jordan West](https://github.com/jrwest) for the structure of his Scaliak driver,
and to [Sandro Gržičić](https://github.com/SandroGrzicic) for the ScalaBuff implementation.

## License
Copyright © 2012 Gideon de Kok

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
