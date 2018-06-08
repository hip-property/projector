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
