// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of gcloud.db;

/// Subclasses of [ModelDescription] describe how to map a dart model object
/// to a Datastore Entity.
///
/// Please see [ModelMetadata] for an example on how to use them.
class ModelDescription {
  final String kind;

  const ModelDescription(this.kind);
}

/// Subclasses of [ExpandoModelDescription] describe how to map a dart expando
/// model object to a Datastore Entity.
///
/// Please see [ModelMetadata] for an example on how to use them.
class ExpandoModelDescription extends ModelDescription {
  const ExpandoModelDescription(String kind) : super(kind);
}
