package com.example.ingest.vector

import java.lang

import mil.nga.giat.geowave.core.index.ByteArrayId
import mil.nga.giat.geowave.core.store.data.field.base.DoubleSerializationProvider
import mil.nga.giat.geowave.core.store.data.field.{FieldWriter, FieldReader, FieldSerializationProviderSpi}

class ScalaDoubleSerializationProvider extends FieldSerializationProviderSpi[Double]{
  val delegate = new DoubleSerializationProvider

//  override def getFieldReader: FieldReader[lang.Double] = delegate.getFieldReader

//  override def getFieldWriter: FieldWriter[AnyRef, lang.Double] = delegate.getFieldWriter
  override def getFieldWriter: FieldWriter[AnyRef, Double] = ???


  override def getFieldReader: FieldReader[Double] = ???
}
