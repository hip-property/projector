package com.hip.projector

import com.hip.projector.aggregators.*
import com.hip.projector.aggregators.Classification.FICTION
import com.hip.projector.aggregators.Classification.NON_FICTION
import com.hip.projector.aggregators.Operation.*
import com.winterbe.expekt.expect
import io.micrometer.core.instrument.Meter
import org.junit.Test
import reactor.test.StepVerifier
import java.time.Duration

internal class ProjectorTest : BaseProjectorTest() {

   @Test
   fun when_eventsForEntitesAreReturned_then_entityIsPresentInProjector() {
      val projector = Projector(spec)

      sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), ADDED))

      expect(projector.size).to.equal(1)
      expect(projector.get(1).title).to.equal("Peter Pan")

      sink.next(SimpleMutationEvent(Book(1, "Peter Pan 2", FICTION,author = Author("J. M. Barrie")), UPDATED))

      // Same id, should have the same size
      expect(projector.size).to.equal(1)
      // but should have updated state
      expect(projector.get(1).title).to.equal("Peter Pan 2")
   }


   @Test(expected = NoSuchElementException::class)
   fun given_entityDoesntExist_when_callingGet_then_exceptionIsThrown() {
      val projector = Projector(spec)
      sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), ADDED))
      projector.get(2)
   }

   @Test
   fun canFilterEvents() {
      val filteredSpec = spec.filter(SimpleMutationEvent::class, { e -> e.book.classification == NON_FICTION })

      val unfilteredProjector = Projector(spec)
      val filteredProjector = Projector(filteredSpec)

      sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), ADDED))
      sink.next(SimpleMutationEvent(Book(id = 2, title = "Teaching Cats origami", classification = NON_FICTION, author = Author("Jimmy Schmitt")), ADDED))


      expect(filteredProjector.size).to.equal(1)
      expect(unfilteredProjector.size).to.equal(2)
   }

   @Test
   fun given_filteringWithATypeThatIsAnInterface_when_classThatImplementsInterfaceIsProvided_then_itIsUsed() {
      val filteredByInterface = spec.filter(UpdateEvent::class, { e -> (e.entity as Book).classification == NON_FICTION })
      val filteredByBaseClass = spec.filter(Any::class, { e -> (e as SimpleMutationEvent).book.classification == NON_FICTION })

      val unfilteredProjector = Projector(spec)
      val filteredProjector1 = Projector(filteredByInterface)
      val filteredProjector2 = Projector(filteredByBaseClass)

      sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), ADDED))
      sink.next(SimpleMutationEvent(Book(id = 2, title = "Teaching Cats Origami", classification = NON_FICTION, author = Author("Jimmy Schmitt")), ADDED))


      expect(filteredProjector1.size).to.equal(1)
      expect(filteredProjector2.size).to.equal(1)
      expect(unfilteredProjector.size).to.equal(2)
   }

   @Test
   fun given_mutatorReturnsNull_then_entryIsRemoved() {
      val projector = Projector(spec)

      sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), ADDED))
      expect(projector.size).to.equal(1)

      sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), REMOVED))
      expect(projector.size).to.equal(0)
   }

   @Test
   fun aProjectorMayBeASourceForAnotherProjector() {

      data class AuthorCount(val author: Author, val count: Int) {
         fun incr() = AuthorCount(author, count + 1)
         fun decr() = AuthorCount(author, count - 1)
      }

      val bookProjector = Projector(spec)

      val authorCountProjector = Projector(
         ProjectorSpec.newSpec<AuthorCount, Author>()
            .addEventSource(bookProjector.eventStream, { e -> e.coalescedState.author }, { currentState, event ->
               when {
                  currentState == null && event.operation == ProjectedMutationEvent.Operation.ADDED -> AuthorCount(event.coalescedState.author, 1)
                  currentState != null && event.operation == ProjectedMutationEvent.Operation.ADDED -> currentState.incr()
                  currentState != null && event.operation == ProjectedMutationEvent.Operation.REMOVED -> currentState.decr()
                  else -> error("Unhandled")
               }
            })
      )

      val jmBarrie = Author("J. M. Barrie")
      sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = jmBarrie), ADDED))
      val jimmySchmitt = Author("Jimmy Schmitt")
      sink.next(SimpleMutationEvent(Book(id = 2, title = "Teaching Cats Origami", classification = NON_FICTION, author = jimmySchmitt), ADDED))

      expect(authorCountProjector.size).to.equal(2)
      expect(authorCountProjector.keys()).to.contain.elements(jmBarrie, jimmySchmitt)
   }

   @Test
   fun given_aQueryIsIssued_then_whenResultsMatch_itIsInvoked() {
      val projector = Projector(spec)

      val queryStream = projector.observe { it.title.contains("Pan") }

      StepVerifier.create(queryStream)
         .then { sink.next(SimpleMutationEvent(Book(id = 2, title = "Teaching Cats Origami", classification = NON_FICTION, author = Author("Jimmy Schmitt")), ADDED)) }
         .expectNoEvent(Duration.ofMillis(100))
         .then { sink.next(SimpleMutationEvent(Book(id = 1, title = "Peter Pan", classification = FICTION, author = Author("J. M. Barrie")), ADDED)) }
         .expectNextMatches { result ->
            expect(result.entity.title).to.equal("Peter Pan")
            expect(result.trigger).to.be.instanceof(SimpleMutationEvent::class.java)
            true
         }
         .thenCancel()
         .verify()
   }

}