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



