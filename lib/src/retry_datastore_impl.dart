// Copyright (c) 2023, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'package:retry/retry.dart';

import '../common.dart';
import '../datastore.dart' as datastore;

/// Datastore implementation which retries most operations
class RetryDatastoreImpl implements datastore.Datastore {
  final datastore.Datastore _delegate;
  final RetryOptions _retryOptions;

  RetryDatastoreImpl(this._delegate, this._retryOptions);

  @override
  Future<List<datastore.Key>> allocateIds(List<datastore.Key> keys) async {
    return await _retryOptions.retry(
      () => _delegate.allocateIds(keys),
      retryIf: _retryIf,
    );
  }

  @override
  Future<datastore.Transaction> beginTransaction({
    bool crossEntityGroup = false,
  }) async {
    return await _retryOptions.retry(
      () => _delegate.beginTransaction(crossEntityGroup: crossEntityGroup),
      retryIf: _retryIf,
    );
  }

  @override
  Future<datastore.CommitResult> commit({
    List<datastore.Entity> inserts = const [],
    List<datastore.Entity> autoIdInserts = const [],
    List<datastore.Key> deletes = const [],
    datastore.Transaction? transaction,
  }) async {
    Future<datastore.CommitResult> fn() async {
      if (transaction == null) {
        return await _delegate.commit(
          inserts: inserts,
          autoIdInserts: autoIdInserts,
          deletes: deletes,
        );
      } else {
        return await _delegate.commit(
          inserts: inserts,
          autoIdInserts: autoIdInserts,
          deletes: deletes,
          transaction: transaction,
        );
      }
    }

    final shouldNotRetry = autoIdInserts.isNotEmpty && transaction == null;
    if (shouldNotRetry) {
      return await fn();
    } else {
      return await _retryOptions.retry(fn, retryIf: _retryIf);
    }
  }

  @override
  Future<List<datastore.Entity?>> lookup(
    List<datastore.Key> keys, {
    datastore.Transaction? transaction,
  }) async {
    return await _retryOptions.retry(
      () async {
        if (transaction == null) {
          return await _delegate.lookup(keys);
        } else {
          return await _delegate.lookup(keys, transaction: transaction);
        }
      },
      retryIf: _retryIf,
    );
  }

  @override
  Future<Page<datastore.Entity>> query(
    datastore.Query query, {
    datastore.Partition? partition,
    datastore.Transaction? transaction,
  }) async {
    Future<Page<datastore.Entity>> fn() async {
      if (partition != null && transaction != null) {
        return await _delegate.query(
          query,
          partition: partition,
          transaction: transaction,
        );
      } else if (partition != null) {
        return await _delegate.query(query, partition: partition);
      } else if (transaction != null) {
        return await _delegate.query(
          query,
          transaction: transaction,
        );
      } else {
        return await _delegate.query(query);
      }
    }

    return await _retryOptions.retry(
      () async => _RetryPage(await fn(), _retryOptions),
      retryIf: _retryIf,
    );
  }

  @override
  Future rollback(datastore.Transaction transaction) async {
    return await _retryOptions.retry(
      () => _delegate.rollback(transaction),
      retryIf: _retryIf,
    );
  }
}

class _RetryPage<K> implements Page<K> {
  final Page<K> _delegate;
  final RetryOptions _retryOptions;

  _RetryPage(this._delegate, this._retryOptions);

  @override
  bool get isLast => _delegate.isLast;

  @override
  List<K> get items => _delegate.items;

  @override
  Future<Page<K>> next({int? pageSize}) async {
    return await _retryOptions.retry(
      () async {
        if (pageSize == null) {
          return await _delegate.next();
        } else {
          return await _delegate.next(pageSize: pageSize);
        }
      },
      retryIf: _retryIf,
    );
  }
}

bool _retryIf(Exception e) {
  if (e is datastore.TransactionAbortedError ||
      e is datastore.NeedIndexError ||
      e is datastore.QuotaExceededError ||
      e is datastore.PermissionDeniedError) {
    return false;
  }
  return true;
}
