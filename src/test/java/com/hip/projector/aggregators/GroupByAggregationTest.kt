package com.hip.projector.aggregators

import com.hip.projector.Projector
import com.hip.projector.ProjectorSpec
import com.hip.projector.aggregators.Classification.*
import com.winterbe.expekt.expect
import org.junit.Test
import org.junit.jupiter.api.Assertions.*
import reactor.test.publisher.TestPublisher

internal class GroupByAggregationTest : BaseProjectorTest() {



   @Test
   fun addingAndRemovingAffectsGroupByCorrectly() {
      val grouping = projector.groupBy { it.classification }
      expect(grouping.keys).to.be.empty

      sink.next(SimpleMutationEvent(books[0],Operation.ADDED))
      expect(grouping.keys).to.contain.elements(FICTION)

      sink.next(SimpleMutationEvent(books[1],Operation.ADDED))
      expect(grouping.keys).to.contain.elements(FICTION)

      expect(grouping[FICTION]).to.contain.elements(books[0],books[1])

      sink.next(SimpleMutationEvent(books[2],Operation.ADDED))
      expect(grouping.keys).to.contain.elements(FICTION, NON_FICTION)

      expect(grouping[NON_FICTION]).to.contain.elements(books[2])

      sink.next(SimpleMutationEvent(books[2],Operation.UPDATED))
      expect(grouping.keys).to.contain.elements(FICTION,NON_FICTION)

      expect(grouping[NON_FICTION]).to.contain.elements(books[2])

      sink.next(SimpleMutationEvent(books[2],Operation.REMOVED))
      expect(grouping.keys).to.contain.elements(FICTION)

      expect(grouping[FICTION]).to.contain.elements(books[0],books[1])
      expect(grouping[NON_FICTION]).to.be.empty


   }


}
