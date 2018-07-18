// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library error_matchers;

import 'dart:io';

import 'package:test/test.dart';
import 'package:gcloud/datastore.dart';

class _ApplicationError extends TypeMatcher<ApplicationError> {
  const _ApplicationError();
}

class _DataStoreError extends TypeMatcher<DatastoreError> {
  const _DataStoreError();
}

class _TransactionAbortedError extends TypeMatcher<TransactionAbortedError> {
  const _TransactionAbortedError();
}

class _NeedIndexError extends TypeMatcher<NeedIndexError> {
  const _NeedIndexError();
}

class _TimeoutError extends TypeMatcher<TimeoutError> {
  const _TimeoutError();
}

class _IntMatcher extends TypeMatcher<int> {
  const _IntMatcher();
}

class _SocketException extends TypeMatcher<SocketException> {
  const _SocketException();
}

const isApplicationError = const _ApplicationError();

const isDataStoreError = const _DataStoreError();
const isTransactionAbortedError = const _TransactionAbortedError();
const isNeedIndexError = const _NeedIndexError();
const isTimeoutError = const _TimeoutError();

const isInt = const _IntMatcher();

const isSocketException = const _SocketException();
