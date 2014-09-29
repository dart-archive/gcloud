// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of gcloud.db;

/// Describes a property of an Entity.
///
/// Please see [ModelMetadata] for an example on how to use them.
abstract class Property {
  /// The name of the property.
  ///
  /// If it is `null`, the name will be the same as used in the
  /// [ModelDescription].
  final String propertyName;

  /// Specifies whether this property is required or not.
  ///
  /// If required is `true`, it will be enforced when saving model objects to
  /// the datastore and when retrieving them.
  final bool required;

  /// Specifies whether this property should be indexed or not.
  ///
  /// When running queries no this property, it is necessary to set [indexed] to
  /// `true`.
  final bool indexed;

  const Property({this.propertyName, this.required: false, this.indexed: true});

  bool validate(ModelDB db, Object value) {
    if (required && value == null) return false;
    return true;
  }

  Object encodeValue(ModelDB db, Object value);

  Object decodePrimitiveValue(ModelDB db, Object value);
}


abstract class PrimitiveProperty extends Property {
  const PrimitiveProperty(
      {String propertyName, bool required: false, bool indexed: true})
      : super(propertyName: propertyName, required: required, indexed: indexed);

  Object encodeValue(ModelDB db, Object value) => value;

  Object decodePrimitiveValue(ModelDB db, Object value) => value;
}


class BoolProperty extends PrimitiveProperty {
  const BoolProperty(
      {String propertyName, bool required: false, bool indexed: true})
      : super(propertyName: propertyName, required: required, indexed: indexed);

  bool validate(ModelDB db, Object value)
      => super.validate(db, value) && (value == null || value is bool);
}

class IntProperty extends PrimitiveProperty {
  const IntProperty(
      {String propertyName, bool required: false, bool indexed: true})
      : super(propertyName: propertyName, required: required, indexed: indexed);

  bool validate(ModelDB db, Object value)
      => super.validate(db, value) && (value == null || value is int);
}

class StringProperty extends PrimitiveProperty {
  const StringProperty(
      {String propertyName, bool required: false, bool indexed: true})
      : super(propertyName: propertyName, required: required, indexed: indexed);

  bool validate(ModelDB db, Object value)
      => super.validate(db, value) && (value == null || value is String);
}

class ModelKeyProperty extends PrimitiveProperty {
  const ModelKeyProperty(
      {String propertyName, bool required: false, bool indexed: true})
      : super(propertyName: propertyName, required: required, indexed: indexed);

  bool validate(ModelDB db, Object value)
      => super.validate(db, value) && (value == null || value is Key);

  Object encodeValue(ModelDB db, Object value) {
    if (value == null) return null;
    return db.toDatastoreKey(value);
  }

  Object decodePrimitiveValue(ModelDB db, Object value) {
    if (value == null) return null;
    return db.fromDatastoreKey(value as datastore.Key);
  }
}

class BlobProperty extends PrimitiveProperty {
  const BlobProperty({String propertyName, bool required: false})
     : super(propertyName: propertyName, required: required, indexed: false);

  // NOTE: We don't validate that the entries of the list are really integers
  // of the range 0..255!
  // If an untyped list was created the type check will always succeed. i.e.
  //   "[1, true, 'bar'] is List<int>" evaluates to `true`
  bool validate(ModelDB db, Object value)
      => super.validate(db, value) && (value == null || value is List<int>);

  Object encodeValue(ModelDB db, Object value) {
      if (value == null) return null;
      return new datastore.BlobValue(value);
  }

  Object decodePrimitiveValue(ModelDB db, Object value) {
    if (value == null) return null;

    datastore.BlobValue blobValue = value;
    return blobValue.bytes;
  }
}

class DateTimeProperty extends PrimitiveProperty {
  const DateTimeProperty(
      {String propertyName, bool required: false, bool indexed: true})
      : super(propertyName: propertyName, required: required, indexed: indexed);

  bool validate(ModelDB db, Object value)
      => super.validate(db, value) && (value == null || value is DateTime);

  Object decodePrimitiveValue(ModelDB db, Object value) {
    if (value is int) {
      return
          new DateTime.fromMillisecondsSinceEpoch(value ~/ 1000, isUtc: true);
    }
    return value;
  }
}


class ListProperty extends Property {
  final PrimitiveProperty subProperty;

  // TODO: We want to support optional list properties as well.
  // Get rid of "required: true" here.
  const ListProperty(this.subProperty,
                     {String propertyName, bool indexed: true})
      : super(propertyName: propertyName, required: true, indexed: indexed);

  bool validate(ModelDB db, Object value) {
    if (!super.validate(db, value) || value is! List) return false;

    for (var entry in value) {
       if (!subProperty.validate(db, entry)) return false;
    }
    return true;
  }

  Object encodeValue(ModelDB db, Object value) {
    if (value == null) return null;
    List list = value;
    if (list.length == 0) return null;
    if (list.length == 1) return list[0];
    return list.map(
        (value) => subProperty.encodeValue(db, value)).toList();
  }

  Object decodePrimitiveValue(ModelDB db, Object value) {
    if (value == null) return [];
    if (value is! List) return [value];
    return (value as List)
        .map((entry) => subProperty.decodePrimitiveValue(db, entry))
        .toList();
  }
}

class StringListProperty extends ListProperty {
  const StringListProperty({String propertyName, bool indexed: true})
      : super(const StringProperty(),
              propertyName: propertyName, indexed: indexed);
}
