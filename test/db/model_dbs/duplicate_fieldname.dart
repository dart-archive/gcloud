// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library gcloud.db.model_test.duplicate_fieldname;

import 'package:gcloud/db.dart' as db;

@db.ModelMetadata(const ADesc())
class A extends db.Model {}

@db.ModelMetadata(const BDesc())
class B extends A {}


class ADesc extends db.ModelDescription {
  final id = const db.IntProperty();

  final foo = const db.IntProperty(propertyName: 'foo');
  const ADesc({String kind: 'A'}) : super(kind);
}

class BDesc extends ADesc {
  final foo = const db.IntProperty(propertyName: 'bar');
  const BDesc() : super(kind: 'B');
}
