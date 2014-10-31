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

import '../common.dart';

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

Future<Storage> connect({bool trace: false}) {
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
        .then((client) {
      if (trace) client = new TraceClient(client);
      return new Storage(client, project);
    });
  });
}

String generateBucketName() {
  var id = new DateTime.now().millisecondsSinceEpoch;
  return 'dart-e2e-test-$id';
}

bool testDetailedApiError(e) => e is common.DetailedApiRequestError;

// Generate a list just above the limit when changing to resumable upload.
const int MB = 1024 * 1024;
const int maxNormalUpload = 1 * MB;
const int minResumableUpload = maxNormalUpload + 1;
var bytesResumableUpload =
    new List.generate(minResumableUpload, (e) => e & 255);

runTests(Storage storage, Bucket testBucket) {
  group('bucket', () {
    test('create-info-delete', () {
      var bucketName = generateBucketName();
      return storage.createBucket(bucketName).then(expectAsync((result) {
        expect(result, isNull);
        return storage.bucketInfo(bucketName).then(expectAsync((info) {
          expect(info.bucketName, bucketName);
          expect(info.etag, isNotNull);
          expect(info.created is DateTime, isTrue);
          expect(info.id, isNotNull);
          return storage.deleteBucket(bucketName)
              .then(expectAsync((result) {
                expect(result, isNull);
              }));
        }));
      }));
    });

    test('create-with-predefined-acl-delete', () {
      Future<Acl> test(predefinedAcl, expectedLength) {
        var bucketName = generateBucketName();
        return storage.createBucket(bucketName, predefinedAcl: predefinedAcl)
            .then(expectAsync((result) {
              expect(result, isNull);
              return storage.bucketInfo(bucketName).then(expectAsync((info) {
                var acl = info.acl;
                expect(info.bucketName, bucketName);
                expect(acl.entries.length, expectedLength);
                return storage.deleteBucket(bucketName)
                    .then(expectAsync((result) {
                      expect(result, isNull);
                    }));
              }));
        }));
      }

      return Future.forEach([
          // TODO: Figure out why some returned ACLs are empty.
          () => test(PredefinedAcl.authenticatedRead, 0),
          // [test, [PredefinedAcl.private, 0]],  // TODO: Cannot delete.
          () => test(PredefinedAcl.projectPrivate, 3),
          () => test(PredefinedAcl.publicRead, 0),
          () => test(PredefinedAcl.publicReadWrite, 0)
      ], (f) => f().then(expectAsync((_) {})));
    });

    test('create-error', () {
      var bucketName = generateBucketName();

      storage.createBucket('goog-reserved').catchError(expectAsync((e) {
        expect(e, isNotNull);
      }), test: testDetailedApiError);
    });
  });

  // TODO: Remove solo_ here when the rate-limit issue have been resolved.
  solo_group('object', () {
    // Run all object tests in the same bucket to try to avoid the rate-limit
    // for creating and deleting buckets while testing.
    Future withTestBucket(function) {
      return function(testBucket).whenComplete(() {
        // TODO: Clean the bucket.
      });
    }

    test('create-read-delete', () {
      Future test(name, bytes) {
      return withTestBucket((Bucket bucket) {
        return bucket.writeBytes('test', bytes).then(expectAsync((info) {
          expect(info, isNotNull);
          return bucket.read('test')
              .fold([], (p, e) => p..addAll(e))
              .then(expectAsync((result) {
                expect(result, bytes);
                return bucket.delete('test').then(expectAsync((result) {
                  expect(result, isNull);
              }));
          }));
        }));
      });
      }

      return Future.forEach([
          () => test('test-1', [1, 2, 3]),
          () => test('test-2', bytesResumableUpload)
     ], (f) => f().then(expectAsync((_) {})));
    });

    test('create-with-predefined-acl-delete', () {
      return withTestBucket((Bucket bucket) {
        Future test(objectName, predefinedAcl, expectedLength) {
          var bucketName = generateBucketName();
          return bucket.writeBytes(
              objectName, [1, 2, 3], predefinedAcl: predefinedAcl)
              .then(expectAsync((result) {
                expect(result, isNotNull);
                return bucket.info(objectName).then(expectAsync((info) {
                  var acl = info.metadata.acl;
                  expect(info.name, objectName);
                  expect(info.etag, isNotNull);
                  expect(acl.entries.length, expectedLength);
                  return bucket.delete(objectName).then(expectAsync((result) {
                    expect(result, isNull);
                  }));
                }));
          }));
        }

        return Future.forEach([
            () => test('test-1', PredefinedAcl.authenticatedRead, 2),
            () => test('test-2', PredefinedAcl.private, 1),
            () => test('test-3', PredefinedAcl.projectPrivate, 4),
            () => test('test-4', PredefinedAcl.publicRead, 2),
            () => test('test-5', PredefinedAcl.bucketOwnerFullControl, 2),
            () => test('test-6', PredefinedAcl.bucketOwnerRead, 2)
        ], (f) => f().then(expectAsync((_) {})));
      });
    });

    test('create-with-acl-delete', () {
      return withTestBucket((Bucket bucket) {
        Future test(objectName, acl, expectedLength) {
          var bucketName = generateBucketName();
          return bucket.writeBytes(objectName, [1, 2, 3], acl: acl)
              .then(expectAsync((result) {
                expect(result, isNotNull);
                return bucket.info(objectName).then(expectAsync((info) {
                  var acl = info.metadata.acl;
                  expect(info.name, objectName);
                  expect(info.etag, isNotNull);
                  expect(acl.entries.length, expectedLength);
                  return bucket.delete(objectName).then(expectAsync((result) {
                    expect(result, isNull);
                  }));
                }));
          }));
        }

        Acl acl1 = new Acl(
            [new AclEntry(AclScope.allAuthenticated, AclPermission.WRITE)]);
        Acl acl2 = new Acl(
            [new AclEntry(AclScope.allUsers, AclPermission.WRITE),
             new AclEntry(new AccountScope('sgjesse@google.com'),
                          AclPermission.WRITE)]);
        Acl acl3 = new Acl(
            [new AclEntry(AclScope.allUsers, AclPermission.WRITE),
             new AclEntry(new AccountScope('sgjesse@google.com'),
                          AclPermission.WRITE),
             new AclEntry(new AccountScope('misc@dartlang.org'),
                          AclPermission.READ)]);
        Acl acl4 = new Acl(
            [new AclEntry(AclScope.allUsers, AclPermission.WRITE),
             new AclEntry(new AccountScope('sgjesse@google.com'),
                          AclPermission.WRITE),
             new AclEntry(new GroupScope('misc@dartlang.org'),
                          AclPermission.READ),
             new AclEntry(new DomainScope('dartlang.org'),
                          AclPermission.FULL_CONTROL)]);

        return Future.forEach([
            () => test('test-1', acl1, 1),
            () => test('test-2', acl2, 2),
            () => test('test-3', acl3, 3),
            () => test('test-4', acl4, 4)
        ], (f) => f().then(expectAsync((_) {})));
      });
    });

    test('create-with-metadata-delete', () {
      return withTestBucket((Bucket bucket) {
        Future test(objectName, metadata, bytes) {
          var bucketName = generateBucketName();
          return bucket.writeBytes(objectName, bytes, metadata: metadata)
              .then(expectAsync((result) {
                expect(result, isNotNull);
                return bucket.info(objectName).then(expectAsync((info) {
                  var acl = info.metadata.acl;
                  expect(info.name, objectName);
                  expect(info.length, bytes.length);
                  expect(info.updated is DateTime, isTrue);
                  expect(info.md5Hash, isNotNull);
                  expect(info.crc32CChecksum, isNotNull);
                  expect(info.downloadLink is Uri, isTrue);
                  expect(info.generation.objectGeneration, isNotNull);
                  expect(info.generation.metaGeneration, 1);
                  expect(info.metadata.contentType, metadata.contentType);
                  expect(info.metadata.cacheControl, metadata.cacheControl);
                  expect(info.metadata.contentDisposition,
                         metadata.contentDisposition);
                  expect(info.metadata.contentEncoding,
                         metadata.contentEncoding);
                  expect(info.metadata.contentLanguage,
                         metadata.contentLanguage);
                  expect(info.metadata.custom, metadata.custom);
                  return bucket.delete(objectName).then(expectAsync((result) {
                    expect(result, isNull);
                  }));
                }));
          }));
        }

        var metadata1 = new ObjectMetadata(contentType: 'text/plain');
        var metadata2 = new ObjectMetadata(
            contentType: 'text/plain',
            cacheControl: 'no-cache',
            contentDisposition: 'attachment; filename="test.txt"',
            contentEncoding: 'gzip',
            contentLanguage: 'da',
            custom: {'a': 'b', 'c': 'd'});

        return Future.forEach([
            () => test('test-1', metadata1, [65, 66, 67]),
            () => test('test-2', metadata2, [65, 66, 67]),
            () => test('test-3', metadata1, bytesResumableUpload),
            () => test('test-4', metadata2, bytesResumableUpload)
        ], (f) => f().then(expectAsync((_) {})));
      });
    });
  });
}

class E2EConfiguration extends SimpleConfiguration {
  Storage storage;
  final String testBucketName;
  E2EConfiguration(this.storage, this.testBucketName): super();

  onDone(success) {
    storage.deleteBucket(testBucketName)
        .whenComplete(() => super.onDone(success));
  }
}

main() {
  // Share the same storage connection for all tests.
  connect(trace: false).then((Storage storage) {
    var bucketName = generateBucketName();
    unittestConfiguration = new E2EConfiguration(storage, bucketName);
    // Create a shared bucket for all object tests.
    storage.createBucket(bucketName).then((result) {
      runTests(storage, storage.bucket(bucketName));
    });
  });
}
