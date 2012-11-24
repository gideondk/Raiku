# Raiku

>Petals of the mountain rose

> Fall now and then,

>To the sound of the waterfall?


## Overview

Raiku is to Riak as a waterfall is to Akka, a simple Riak client which enables you to let your Riak data flow to your Scala applications.

It's targeted as a non-blocking, performance focused, full-scale alternative to the java Riak driver. 

Based on Akka IO, it uses the iteratee pattern and actors to create the best throughput possible.

## Status

The client should currently treated as a proof of concept, but is stable enough to try out in hobby-projects.

**You can use the following in the client:**

*  Writing low-level protobuf style read-write objects through a RaikuClient;
* Doing this non-blocking through multiple sockets, handled by a single actor;
* Writing, fetching and deleting single or multiple objects at once;
* Querying items on 2i, based on binary or integral indexes (ranges also supported);
* Sequencing and continuing multiple operations using monad transformers (ValidatedFuture, ValidatedFutureIO).

**The following is currently missing in the client, and will be added soon:**

* Map/Reduce functionality;
* Link walking;
* Custom mutations / conflict resolutions;
* Connection pooling;
* Retriers;
* Durable mailboxes;
* Least-connection-error-based router / pool;
* Fully iteratee-based reactive-mongo like goodness.

## Architecture

The client is uses Akka IO and iteratees to send and receive protocol buffer encoded data streams over "normal" TCP sockets.

Protocol Buffer messages are transformed into easier to use requests and back, Riak PBC Content objects are serialised into *RWObjects*, which are easy to use case classes, containing all information needed to successfully write objects back into Riak.

You can use the client functionality to fetch, store and delete these "low level" objects, but it's wiser to use the RaikuBucket to store objects converted using a RaikuConverter implementation. 

You are free to use any value serialisation method available, but I recommended to use the Spray JSON package (behaves very good in multi-threaded environments).

All operations return a value in a monad transformer (ValidatedFutureIO) which combines a validation, future and IO monad into one type: most (if not all) exceptions will be caught in the validation monad, all async actions are abstracted into a future monad and all IO actions are as pure as possible by using the Scalaz IO monad.

## DSL
You can use the *normal* functions to store, fetch or delete objects. 

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
