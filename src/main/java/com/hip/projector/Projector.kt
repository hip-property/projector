package com.hip.projector

import org.reactivestreams.Publisher
import reactor.core.publisher.DirectProcessor
import reactor.core.publisher.Flux
import kotlin.reflect.KClass
import kotlin.reflect.full.isSuperclassOf
import kotlin.reflect.jvm.reflect


/**
 * Defines a set of event sources which will provide data to a projector.
 *
 * The intent is that projectSpecs are shareable, at least as base recipies, which
 * can then be further customized as needs dictate.
 */
class ProjectorSpec<TEntity, ID> private constructor(val eventSources: List<EventSource<Any, TEntity, ID>>) {
   companion object {
      fun <TEntity, ID> newSpec(): ProjectorSpec<TEntity, ID> = ProjectorSpec(emptyList())
   }

   fun <TEvent> addEventSource(eventSource: Flux<TEvent>, keyExtractor: KeyExtractor<TEvent, ID>, mutator: Mutator<TEntity, TEvent>): ProjectorSpec<TEntity, ID> {
      return addEventSource(EventSource(eventSource, keyExtractor, mutator))
   }

   @Suppress("UNCHECKED_CAST")
   fun <E> addEventSource(eventSource: EventSource<E, TEntity, ID>): ProjectorSpec<TEntity, ID> {
      return ProjectorSpec((eventSources + eventSource) as List<EventSource<Any, TEntity, ID>>)
   }

   @Suppress("UNCHECKED_CAST")
   fun <E : Any> filter(kClass: KClass<E>, predicate: (E) -> Boolean): ProjectorSpec<TEntity, ID> {
      val updatedEventSources = this.eventSources.map {

         val classifier = it.keyExtractor.reflect()!!.parameters[0].type.classifier as KClass<*>

         if (kClass.isSuperclassOf(classifier)) {
            (it as EventSource<E, TEntity, ID>).filter(predicate)
         } else {
            it
         }
      } as List<EventSource<Any, TEntity, ID>>
      return ProjectorSpec(updatedEventSources)
   }

   fun projector(): Projector<TEntity, ID> = Projector(this)
}

data class ProjectedMutationEvent<TEntity, ID>(
   val id: ID,
   val previousState: TEntity?,
   val currentState: TEntity?,
   val trigger: Any
) {
   enum class Operation {
      ADDED,REMOVED,UPDATED
   }
   val coalescedState: TEntity = currentState ?: previousState ?:
      error("Neither current state or previous state is populated - this shoulnd't happen")
   val operation:Operation = when {
      previousState == null && currentState != null -> Operation.ADDED
      previousState != null && currentState == null -> Operation.REMOVED
      previousState != null && currentState != null -> Operation.UPDATED
      else -> error("Unhandled, but this shouldn't happen")
   }

}

object StateOfTheWorldMarker

/**
 * Gives a repository type api to a stream of data.
 * Uses a ProjectorSpec to define how materialized views are constructed.
 * Projectors themselves may also be inputs into other projectors
 */
open class Projector<TEntity, ID>(spec: ProjectorSpec<TEntity, ID>) {
   private val dataTable = mutableMapOf<ID, TEntity>()
   private val eventStreamProcessor: DirectProcessor<ProjectedMutationEvent<TEntity, ID>> = DirectProcessor.create()
   private val eventStreamSink = eventStreamProcessor.sink()

   val eventStream: Flux<ProjectedMutationEvent<TEntity, ID>>
      get() = eventStreamProcessor


   init {
      spec.eventSources.map { (eventSource, keyExtractor, mutator) ->
         eventSource.subscribe { event ->
            if (event != null) {
               val key = keyExtractor(event)
               val originalState = dataTable[key]
               val updatedState = mutator(originalState, event)
               if (updatedState != null) {
                  this.dataTable[key] = updatedState
               } else {
                  this.dataTable.remove(key)
               }
               if (originalState != null || updatedState != null) {
                  eventStreamSink.next(ProjectedMutationEvent(key, originalState, updatedState, event))
               }
            }

         }
      }
   }


   fun get(id: ID): TEntity {
      return dataTable[id] ?: throw NoSuchElementException("No entity with id of $id")
   }

   fun observe(predicate: (TEntity) -> Boolean): Flux<QueryResult<TEntity>> {
      return eventStream
         .filter { mutationEvent -> mutationEvent.currentState != null && predicate(mutationEvent.currentState) }
         .map { mutationEvent -> QueryResult(mutationEvent.currentState!!, mutationEvent.trigger) }
   }

   fun queryAndObserve(predicate: (TEntity) -> Boolean): Flux<QueryResult<TEntity>> {
      return Flux.fromIterable(query(predicate))
         .map { entity -> QueryResult(entity, StateOfTheWorldMarker) }
         .concatWith(observe(predicate))
   }

   fun query(predicate: (TEntity) -> Boolean): List<TEntity> {
      return this.getAll().filter(predicate)
   }

   fun getAll(): List<TEntity> {
      return dataTable.values.toList()
   }

   fun keys(): Set<ID> {
      return dataTable.keys.toSet()
   }

   fun contains(entityId: ID): Boolean {
      return dataTable.keys.contains(entityId)
   }


   val size: Int
      get() = dataTable.size

   val isEmpty
      get() = dataTable.isEmpty()

   val isNotEmpty
      get() = dataTable.isNotEmpty()


}


/**
 * A function which receives an existing state of an entity
 * (if present) and a mutating event, and returns the new updated state (or null,
 * to indicate the entity should be deleted)
 */
typealias Mutator<TEntity, TEvent> = (TEntity?, TEvent) -> TEntity?

/**
 * A function which receives an event, and returns the entity id that
 * the event relates to
 */
typealias KeyExtractor<TEvent, ID> = (TEvent) -> ID

/**
 * Provides an input of events, along with instructions for how the projector should
 * decide which entity the event relates to (the KeyExtractor), and how the event should
 * change the state of the entity (the mutator)
 */
data class EventSource<TEvent, TEntity, ID>(val source: Flux<TEvent>, val keyExtractor: KeyExtractor<TEvent, ID>, val mutator: Mutator<TEntity, TEvent>) : Publisher<TEvent> by source {
   fun filter(predicate: (TEvent) -> Boolean): EventSource<TEvent, TEntity, ID> {
      return EventSource(source.filter(predicate), keyExtractor, mutator)
   }
}

data class QueryResult<out TEntity>(val entity: TEntity, val trigger: Any)


fun <TEntity, ID> Flux<ProjectedMutationEvent<TEntity, ID>>.eventSource(): EventSource<ProjectedMutationEvent<TEntity, ID>, TEntity, ID> {
   return EventSource(source = this,
      keyExtractor = { mutationEvent -> mutationEvent.id },
      mutator = { _, event -> event.currentState }
   )
}

fun <TEntity, ID> Flux<ProjectedMutationEvent<TEntity, ID>>.projector(): Projector<TEntity, ID> {
   return Projector(ProjectorSpec.newSpec<TEntity, ID>()
      .addEventSource(this.eventSource()))
}


fun <TEvent, TEntity, TId> Flux<TEvent>.toEventSource(keyExtractor: KeyExtractor<TEvent, TId>, mutator: Mutator<TEntity, TEvent>): EventSource<TEvent, TEntity, TId> {
   return EventSource(this, keyExtractor, mutator)
}

fun <TEvent, TEntity, TId> EventSource<TEvent, TEntity, TId>.toProjectorSpec(): ProjectorSpec<TEntity, TId> {
   return ProjectorSpec.newSpec<TEntity, TId>()
      .addEventSource(this)
}
