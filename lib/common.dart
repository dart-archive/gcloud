// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library gcloud.pubsub;

import 'dart:async';

/// A single page of paged results from a query.
///
/// Use `next` to move to the next page. If this is the last page `next`
/// completes with `null`
abstract class Page<T> {
  /// The items in this page.
  List<T> get items;

  /// Whether this is the last page of results.
  bool get isLast;

  /// Move to the next page.
  ///
  /// The future returned completes with the next page or results.
  ///
  /// If [next] is called on the last page the returned future completes
  /// with `null`.
  Future<Page<T>> next({int pageSize});
}