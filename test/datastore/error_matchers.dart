// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library error_matchers;

import 'dart:io';

import 'package:test/test.dart';
import 'package:gcloud/datastore.dart';

const isApplicationError = const TypeMatcher<ApplicationError>();

const isDataStoreError = const TypeMatcher<DatastoreError>();
const isTransactionAbortedError = const TypeMatcher<TransactionAbortedError>();
const isNeedIndexError = const TypeMatcher<NeedIndexError>();
const isTimeoutError = const TypeMatcher<TimeoutError>();

const isInt = const TypeMatcher<int>();

const isSocketException = const TypeMatcher<SocketException>();
