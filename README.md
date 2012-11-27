# Raiku

>Petals of the mountain rose

>Fall now and then,

>To the sound of the waterfall?


## Overview

Raiku is to Riak as a waterfall is to Akka; a simple Riak client which lets Riak flow to your Scala applications.

It's targeted as a non-blocking, performance focused, full-scale alternative to the java Riak driver. 

Based on Akka IO, it uses the iteratee pattern and actors to create the best throughput possible.

## Status

The client should currently treated as a proof of concept, but is stable enough to try out in hobby-projects.

**Currently available in the client:**

* Writing low-level protobuf style read-write objects through a RaikuClient;
* Doing this non-blocking through multiple sockets, handled by a single actor;
* Writing, fetching and deleting single or multiple objects at once;
* Querying items on 2i, based on binary or integral indexes (ranges also supported);
* Sequencing and continuing multiple operations using monad transformers (ValidatedFuture, ValidatedFutureIO).

**The following is currently missing in the client, but will be added soon:**

* Map/Reduce functionality;
* Link walking;
* Custom mutations / conflict resolutions;
* Connection pooling;
* Retriers;
* Durable mailboxes;
* Least-connection-error-based router / pool;
* Full iteratee-based reactive-mongo like goodness.

## Architecture

The client uses Akka IO and iteratees to send and receive protocol buffer encoded data streams over TCP sockets.

Protocol Buffer messages are transformed into case classes using ScalaBuff, Riak PBC Content classes are serialized into *RWObjects*, which are case classes, containing all information needed to write objects back into Riak.

You can use the client to fetch, store and delete these "low level" objects, but it's wiser to use the RaikuBucket to store objects converted using a RaikuConverter implementation. 

You are free to use any value serialisation method available, but I recommended to use the Spray JSON package (behaves very good in multi-threaded environments).

All operations return a value in a monad transformer (<code>ValidatedFutureIO</code>) which combines a <code>Validation</code>, <code>Future</code> and <code>IO</code> monad into one type: most (if not all) exceptions will be caught in the validation monad, all async actions are abstracted into a future monad and all IO actions are as pure as possible by using the Scalaz IO monad.

Use <code>unsafePerformIO</code> to expose the Future, or use <code>unsafeFulFill(d: Duration)</code> to perform IO and wait (blocking) on the future.

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
<pre><code>persons ? 	personId
persons ?* 	List(personIdA, personIdB)
</code></pre>

**Storing objects**
<notextile><pre><code>persons <<	Person("Basho", 42, "Japan")
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

## Monadic behavior
You can use the monadic behavior of <code>ValidatedFutureIO[T]</code> to combine multiple requests:

<pre><code>val objs: ValidatedFutureIO[List[Person]] = for {
	keys <- persons idx ("age", 39 to 50)
	objs <- persons ?* keys
} yield objs

</code></pre>

Or better, using Scalaz:

<pre><code>persons idx ("age", 39 to 50) >>= ((x: List[String]) => bucket ?* x)</code></pre>

Or if you want to run queries in parrallel:

<pre><code>val storePersons = persons <<* perObjs
val storeCountries = countries <<* countryObjs

ValidatedFutureIO.sequence(List(storePersons, storeCountries))</code></pre>


## Credits

Lots of credits go to [Jordan West](https://github.com/jrwest) for the structure of his Scaliak driver,
and to [Sandro Gržičić](https://github.com/SandroGrzicic) for the ScalaBuff implementation.

## License
Copyright © 2012 Gideon de Kok

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.