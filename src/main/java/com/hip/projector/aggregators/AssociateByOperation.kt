package com.hip.projector.aggregators

import com.hip.projector.*
import reactor.core.publisher.Flux

/**
 * Re-maps an entity + key projection
 * to another key for the same entity.
 *
 * Useful for taking a projection that's been mapped by an ID,
 * and re-mapping it by another attribute.
 *
 */
class AssociateByOperation<TEntity, TId1, TId2>(
   projector: Projector<TEntity, TId1>,
   association: (TEntity) -> TId2
) {

   val eventStream = projector.eventStream
      .map { ProjectedMutationEvent(association(it.coalescedState), it.previousState, it.currentState, it.trigger) }
}

fun <TEntity, TId1, TId2> Projector<TEntity, TId1>.associateBy(association: (TEntity) -> TId2): Projector<TEntity, TId2> {
   val spec = ProjectorSpec.newSpec<TEntity, TId2>()
      .addEventSource(AssociateByOperation(this, association).eventStream.eventSource())
   return Projector(spec)
}



