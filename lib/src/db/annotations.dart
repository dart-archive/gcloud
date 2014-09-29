// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of gcloud.db;

/// This class should be used to annotate DB Model classes.
///
/// It will attach a description on how to map dart Objects to Datastore
/// Entities.
///
/// Note that the model class needs to have an empty default constructor.
///
/// Here is an example of a Dart Model class and a ModelScription which
/// describes the mapping.
///
///     @ModelMetadata(const PersonDesc())
///     class Person extends Model {
///       String name;
///       DateTime dateOfBirth;
///     }
///
///     class PersonDesc extends ModelDescription {
///       final id = const IntProperty();
///
///       final name = const StringProperty();
///       final dateOfBirth = const DateTimeProperty();
///
///       const GreetingDesc() : super('Person');
///     }
///
class ModelMetadata {
  final ModelDescription description;

  const ModelMetadata(this.description);
}
