// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library gcloud.db.model_test.duplicate_property;

import 'package:gcloud/db.dart' as db;

@db.ModelMetadata(const ADesc())
class A extends db.Model { }

class ADesc extends db.ModelDescription {
  final id = const db.IntProperty();
  final foo = const db.IntProperty(propertyName: 'foo');
  final bar = const db.IntProperty(propertyName: 'foo');
  const ADesc() : super('A');
}
