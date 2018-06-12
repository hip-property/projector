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

import com.winterbe.expekt.expect
import org.junit.Test


internal class AssociateByOperationTest : BaseProjectorTest() {

   @Test
   fun addingAndRemovingAffectsGroupByCorrectly() {
      val classifcations = projector.associateBy { it.classification }
      expect(classifcations.isEmpty).to.be.`true`

      sink.next(SimpleMutationEvent(books[0],Operation.ADDED))
      expect(classifcations.keys()).to.contain.all.elements(Classification.FICTION)

      sink.next(SimpleMutationEvent(books[2],Operation.ADDED))
      expect(classifcations.keys()).to.contain.all.elements(Classification.FICTION, Classification.NON_FICTION)
   }
}
