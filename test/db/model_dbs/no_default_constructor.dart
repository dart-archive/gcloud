// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library gcloud.db.model_test.no_default_constructor;

import 'package:gcloud/db.dart' as db;

@db.ModelMetadata(const ADesc())
class A extends db.Model {
  A(int i);
}

class ADesc extends db.ModelDescription {
  final id = const db.IntProperty();
  const ADesc() : super('A');
}
