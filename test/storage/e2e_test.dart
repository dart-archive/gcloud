// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library gcloud.storage;

import 'dart:async';
import 'dart:io';

import 'package:gcloud/storage.dart';
import 'package:googleapis/common/common.dart' as common;
import 'package:googleapis_auth/auth_io.dart' as auth;
import 'package:unittest/unittest.dart';


// Enviroment variables for specifying the cloud project to use and the
// location of the service account key for that project.
const String PROJECT_ENV = 'GCLOUD_E2E_TEST_PROJECT';
const String SERVICE_KEY_LOCATION_ENV = 'GCLOUD_E2E_TEST_KEY';

// Default project and service key location used when running on the package
// bot.
const String DEFAULT_PROJECT = 'dart-gcloud-e2e';
const String DEFAULT_KEY_LOCATION =
    'gs://dart-archive-internal/keys/dart-gcloud-e2e.json';

bool onBot() {
  // When running on the package-bot the current user is chrome-bot.
  var envName;
  if (Platform.isWindows) {
    envName = 'USERNAME';
  } else {
    envName = 'USER';
  }
  return Platform.environment[envName] == 'chrome-bot';
}

// Get the service key from the specified location.
Future<String> serviceKeyJson(String serviceKeyLocation) {
  if (!serviceKeyLocation.startsWith('gs://')) {
    throw new Exception('Service key location must start with gs://');
  }
  var future;
  if (onBot()) {
    future = Process.run(
        'python', ['third_party/gsutil/gsutil', 'cat', serviceKeyLocation],
        runInShell: true);
  } else {
    var gsutil = Platform.isWindows ? 'gsutil.cmd' : 'gsutil';
    future = Process.run(gsutil, ['cat', serviceKeyLocation]);
  }
  return future.then((result) {
    if (result.exitCode != 0) {
      throw new Exception('Failed to run gsutil, ${result.stderr}');
    }
    return  result.stdout;
  });
}

Future<Storage> connect() {
  String project = Platform.environment[PROJECT_ENV];
  String serviceKeyLocation = Platform.environment[SERVICE_KEY_LOCATION_ENV];

  if (!onBot() && (project == null || serviceKeyLocation == null)) {
    throw new StateError(
        'Envoronment variables $PROJECT_ENV and $SERVICE_KEY_LOCATION_ENV '
        'required when not running on the package bot');
  }

  project = project != null ? project : DEFAULT_PROJECT;
  serviceKeyLocation =
      serviceKeyLocation != null ? serviceKeyLocation : DEFAULT_KEY_LOCATION;

  return serviceKeyJson(serviceKeyLocation).then((keyJson) {
    var creds = new auth.ServiceAccountCredentials.fromJson(keyJson);
    return auth.clientViaServiceAccount(creds, Storage.Scopes)
        .then((client) => new Storage(client, project));
  });
}

String generateBucketName() {
  var id = new DateTime.now().millisecondsSinceEpoch;
  return 'dart-e2e-test-$id';
}

bool testDetailedApiError(e) => e is common.DetailedApiRequestError;

runTests(Storage storage) {
  group('bucket', () {

    test('create-delete', () {
      var bucketName = generateBucketName();

      storage.createBucket(bucketName).then(expectAsync((result) {
        expect(result, isNull);
        expect(storage.deleteBucket(bucketName), completion(isNull));
      }));
    });

    test('create-error', () {
      var bucketName = generateBucketName();

      storage.createBucket('goog-reserved').catchError(expectAsync((e) {
        expect(e, isNotNull);
      }), test: testDetailedApiError);
    });
  });
}

main() {
  // Share the same storage connection for all tests.
  connect().then(runTests);
}
