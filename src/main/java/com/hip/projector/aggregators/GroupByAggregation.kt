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

import com.hip.projector.ProjectedMutationEvent
import com.hip.projector.Projector

interface Aggregation<TEntity, T>

/**
 * Groups an aggregation by the provided key extractor
 */
class GroupByAggregation<TEntity, T>(
   projector: Projector<TEntity, *>,
   grouper: (TEntity) -> T
) : Aggregation<TEntity, T> {

   private val entries: MutableMap<T, MutableList<TEntity>> = mutableMapOf()

   init {
      projector.eventStream.subscribe { mutation ->
         val groupByValue = grouper(mutation.coalescedState)

         // TODO : At this point, we could also emit an event stream
         // for adds / removes, which may be useful for people to react to
         entries.compute(groupByValue, { _: T, valueList ->
            val listWithUpdate = (valueList ?: mutableListOf())
            val updatedList = when (mutation.operation) {
               ProjectedMutationEvent.Operation.REMOVED -> listWithUpdate.remove(mutation.previousState!!)
               ProjectedMutationEvent.Operation.ADDED -> listWithUpdate.add(mutation.currentState!!)
               ProjectedMutationEvent.Operation.UPDATED -> {
                  listWithUpdate.remove(mutation.previousState)
                  listWithUpdate.add(mutation.currentState!!)
               }
            }
            if (listWithUpdate.isEmpty()) null else listWithUpdate
         })
      }
   }

   val keys: Set<T>
      get() = entries.keys

   operator fun get(key: T): List<TEntity> {
      return entries.get(key) ?: emptyList()
   }

   fun containsKey(entry: T): Boolean = keys.contains(entry)
}

fun <TEntity, TID, TGroupValue> Projector<TEntity, TID>.groupBy(grouper: (TEntity) -> TGroupValue): GroupByAggregation<TEntity, TGroupValue> {
   return GroupByAggregation(this, grouper)
}

