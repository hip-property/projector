# Projector

Projector provides a simple way to create projections from streams of data

It's designed to allow the sharable creation of `ProjectorSpec`'s, which can be defined in a common
library and shared between multiple consumers.

This segregates the definition of how to use a stream o fdata, from the actual consumption of one.

Here's an example:

```kotlin
val books = listOf(Book(1, "Peter Pan", Classification.FICTION, Author("J. M. Barrie")), Book(2, "Eat Love Pray", Classification.FICTION, Author("Julia Roberts")), Book(3, "Clean Code", Classification.NON_FICTION, Author("Uncle Bob")))

val spec: ProjectorSpec<Book, Int> = ProjectorSpec.newSpec<Book, Int>()
      .addEventSource(
         eventSource = someFlux,
         keyExtractor = { e -> e.book.id },
         mutator = { _, event ->
            if (event.operation == Operation.REMOVED) null else event.book
         }
      )
      
val projector = Projector(spec)

// Assuming some data:
sink.next(SimpleMutationEvent(Book(id = 2, title = "Teaching Cats Origami", classification = NON_FICTION, author = Author("Jimmy Schmitt")), ADDED))
sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), ADDED))


// now you have a projector, you can query it:
val allCurrentBooks = projector.getAll()
val peterPan = projector.get(1)

// you can write a more interesting query of current state:
val books:List<Book> = projector.query { it.title.contains("Pan") }

// or get a continuous stream of data:
val queryStream: Flux<QueryResult<Book>> = projector.observe { it.title.contains("Pan") }

```
