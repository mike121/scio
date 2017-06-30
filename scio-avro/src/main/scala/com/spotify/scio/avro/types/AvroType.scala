/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.avro.types

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.runtime.universe._

object AvroType {
  class fromSchema(schema: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.schemaImpl
  }

  class toSchema extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.toSchemaImpl
  }

  trait HasAvroSchema[T] {
    def schema: Schema

    def fromGenericRecord: (GenericRecord => T)

    def toGenericRecord: (T => GenericRecord)

    def toPrettyString(indent: Int = 0): String
  }

  trait HasAvroDoc {
    def doc: String
  }

  trait HasAvroAnnotation

  def schemaOf[T: TypeTag]: Schema = SchemaProvider.schemaOf[T]

  def fromGenericRecord[T]: (GenericRecord => T) = macro ConverterProvider.fromGenericRecordImpl[T]

  def toGenericRecord[T]: (T => GenericRecord) = macro ConverterProvider.toGenericRecordImpl[T]

  def apply[T: TypeTag]: AvroType[T] = new AvroType[T]
}

class AvroType[T: TypeTag] {
  private val instance = runtimeMirror(getClass.getClassLoader)
    .reflectModule(typeOf[T].typeSymbol.companion.asModule)
    .instance

  private def getField(key: String) = instance.getClass.getMethod(key).invoke(instance)

  def fromGenericRecord: (GenericRecord => T) =
    getField("fromGenericRecord").asInstanceOf[(GenericRecord => T)]

  def toGenericRecord: (T => GenericRecord) =
    getField("toGenericRecord").asInstanceOf[(T => GenericRecord)]

  def schema: Schema = AvroType.schemaOf[T]
}
