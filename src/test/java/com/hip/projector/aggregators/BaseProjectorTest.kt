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
import reactor.test.publisher.TestPublisher

abstract class BaseProjectorTest {
   val books = listOf(Book(1, "Peter Pan", Classification.FICTION, Author("J. M. Barrie")), Book(2, "Eat Love Pray", Classification.FICTION, Author("Julia Roberts")), Book(3, "Clean Code", Classification.NON_FICTION, Author("Uncle Bob")))
   val sink = TestPublisher.create<SimpleMutationEvent>()
   val spec: ProjectorSpec<Book, Int> = ProjectorSpec.newSpec<Book, Int>()
      .addEventSource(
         eventSource = sink.flux(),
         keyExtractor = { e -> e.book.id },
         mutator = { _, event ->
            if (event.operation == Operation.REMOVED) null else event.book
         }
      )

   val projector = Projector(
      spec
   )
}


data class SimpleMutationEvent(val book: Book, val operation: Operation) : UpdateEvent<Book> {
   override val entity: Book = book
}

data class Book(val id: Int, val title: String, val classification: Classification, val author: Author)
data class Author(val name: String)
enum class Classification { FICTION, NON_FICTION }
enum class Operation { ADDED, UPDATED, REMOVED }

interface UpdateEvent<T> {
   val entity: T
}
