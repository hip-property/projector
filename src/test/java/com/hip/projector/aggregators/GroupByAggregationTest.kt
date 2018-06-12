/*-
 * =========================================================BeginLicense
 * Projector
 * .
 * Copyright (C) 2018 HiP Property
 * .
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ===========================================================EndLicense
 */
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
