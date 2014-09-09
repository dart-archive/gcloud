// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library gcloud.datastore;

import 'dart:async';

class DatastoreError implements Exception {
  final String message;

  DatastoreError([String message]) : message =
      (message != null ?message : 'DatastoreError: An unknown error occured');
}

class UnknownDatastoreError extends DatastoreError {
  UnknownDatastoreError(error) : super("An unknown error occured ($error).");
}

class TransactionAbortedError extends DatastoreError {
  TransactionAbortedError() : super("The transaction was aborted.");
}

class TimeoutError extends DatastoreError {
  TimeoutError() : super("The operation timed out.");
}

class NeedIndexError extends DatastoreError {
  NeedIndexError()
      : super("An index is needed for the query to succeed.");
}

class PermissionDeniedError extends DatastoreError {
  PermissionDeniedError() : super("Permission denied.");
}

class InternalError extends DatastoreError {
  InternalError() : super("Internal service error.");
}

class QuotaExceededError extends DatastoreError {
  QuotaExceededError(error) : super("Quota was exceeded ($error).");
}


class Entity {
  final Key key;
  final Map<String, Object> properties;
  final Set<String> unIndexedProperties;

  Entity(this.key, this.properties, {this.unIndexedProperties});
}

class Key {
  final Partition partition;
  final List<KeyElement> elements;

  Key(this.elements, {Partition partition})
      : this.partition = (partition == null) ? Partition.DEFAULT : partition;

  factory Key.fromParent(String kind, int id, {Key parent}) {
    var partition;
    var elements = [];
    if (parent != null) {
      partition = parent.partition;
      elements.addAll(parent.elements);
    }
    elements.add(new KeyElement(kind, id));
    return new Key(elements, partition: partition);
  }

  int get hashCode =>
      elements.fold(partition.hashCode, (a, b) => a ^ b.hashCode);

  bool operator==(Object other) {
    if (identical(this, other)) return true;

    if (other is Key &&
        partition == other.partition &&
        elements.length == other.elements.length) {
      for (int i = 0; i < elements.length; i++) {
        if (elements[i] != other.elements[i]) return false;
      }
      return true;
    }
    return false;
  }

  String toString() {
    var namespaceString =
        partition.namespace == null ? 'null' : "'${partition.namespace}'";
    return "Key(namespace=$namespaceString, path=[${elements.join(', ')}])";
  }
}

class Partition {
  static const Partition DEFAULT = const Partition._default();

  final String namespace;

  Partition(this.namespace) {
    if (namespace == '') {
      throw new ArgumentError("'namespace' must not be empty");
    }
  }

  const Partition._default() : this.namespace = null;

  int get hashCode => namespace.hashCode;

  bool operator==(Object other) =>
      other is Partition && namespace == other.namespace;
}

class KeyElement {
  final String kind;
  final id; // either int or string

  KeyElement(this.kind, this.id) {
    if (kind == null) {
      throw new ArgumentError("'kind' must not be null");
    }
    if (id != null) {
      if (id is! int && id is! String) {
        throw new ArgumentError("'id' must be either null, a String or an int");
      }
    }
  }

  int get hashCode => kind.hashCode ^ id.hashCode;

  bool operator==(Object other) =>
      other is KeyElement && kind == other.kind && id == other.id;

  String toString() => "$kind.$id";
}

class FilterRelation {
  static const FilterRelation LessThan = const FilterRelation._('<');
  static const FilterRelation LessThanOrEqual = const FilterRelation._('<=');
  static const FilterRelation GreatherThan = const FilterRelation._('>');
  static const FilterRelation GreatherThanOrEqual =
      const FilterRelation._('>=');
  static const FilterRelation Equal = const FilterRelation._('==');
  static const FilterRelation In = const FilterRelation._('IN');

  final String name;
  const FilterRelation._(this.name);
}

class Filter {
  final FilterRelation relation;
  final String name;
  final Object value;

  Filter(this.relation, this.name, this.value);
}

class OrderDirection {
  static const OrderDirection Ascending = const OrderDirection._('Ascending');
  static const OrderDirection Decending = const OrderDirection._('Decending');

  final String name;
  const OrderDirection._(this.name);
}

class Order {
  final OrderDirection direction;
  final String propertyName;

  Order(this.direction, this.propertyName);
}

class Query {
  final String kind;
  final Key ancestorKey;
  final List<Filter> filters;
  final List<Order> orders;
  final int offset;
  final int limit;

  Query({this.ancestorKey, this.kind, this.filters, this.orders,
         this.offset, this.limit});
}

class CommitResult {
  final List<Key> autoIdInsertKeys;

  CommitResult(this.autoIdInsertKeys);
}

class BlobValue {
  final List<int> bytes;
  BlobValue(this.bytes);
}

abstract class Transaction { }

abstract class Datastore {
  Future<List<Key>> allocateIds(List<Key> keys);

  Future<Transaction> beginTransaction({bool crossEntityGroup: false});

  // Can throw a [TransactionAbortedError] error.
  Future<CommitResult> commit({List<Entity> inserts,
                               List<Entity> autoIdInserts,
                               List<Key> deletes,
                               Transaction transaction});
  Future rollback(Transaction transaction);

  Future<List<Entity>> lookup(List<Key> keys, {Transaction transaction});
  Future<List<Entity>> query(
      Query query, {Partition partition, Transaction transaction});
}
