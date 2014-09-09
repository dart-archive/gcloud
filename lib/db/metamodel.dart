// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library gcloud.db.meta_model;

import '../db.dart';

@ModelMetadata(const NamespaceDescription())
class Namespace extends ExpandoModel {
  String get name {
    // The default namespace will be reported with id 1.
    if (id == NamespaceDescription.EmptyNamespaceId) return null;
    return id;
  }
}

@ModelMetadata(const KindDescription())
class Kind extends Model {
  String get name => id;
}

class NamespaceDescription extends ExpandoModelDescription {
  static const int EmptyNamespaceId = 1;
  final id = const IntProperty();
  const NamespaceDescription() : super('__namespace__');
}

class KindDescription extends ModelDescription {
  final id = const IntProperty();
  const KindDescription() : super('__kind__');
}
