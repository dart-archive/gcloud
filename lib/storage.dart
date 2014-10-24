// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

/// This library provides access to Google Cloud Storage.
///
/// Google Cloud Storage is an object store for binary objects. Each
/// object has a set of metadata attached to it. For more information on
/// Google Cloud Sorage see https://developers.google.com/storage/.
///
/// There are two main concepts in Google Cloud Storage: Buckets and Objects.
/// A bucket is a container for objects and objects are the actual binary
/// objects.
///
/// The API has two main classes for dealing with buckets and objects.
///
/// The class `Storage` is the main API class providing access to working
/// with buckets. This is the 'bucket service' interface.
///
/// The class `Bucket` provide access to working with objcts in a specific
/// bucket. This is the 'object service' interface.
///
/// Both buckets have objects, have names. The bucket namespace is flat and
/// global across all projects. This means that a bucket is always
/// addressable using its name without requiring further context.
///
/// Within buckets the object namespace is also flat. Object are *not*
/// organized hierachical. However, as object names allow the slash `/`
/// character this is often used to simulate a hierachical structure
/// based on common prefixes.
///
/// This package uses relative and absolute names to refer to objects. A
/// relative name is just the object name within a bucket, and requires the
/// context of a bucket to be used. A relative name just looks like this:
///
///     object_name
///
/// An absolute name includes the bucket name and uses the `gs://` prefix
/// also used by the `gsutil` tool. An absolute name looks like this.
///
///     gs://bucket_name/object_name
///
/// In most cases relative names are used. Absolute names are typically
/// only used for operations involving objects in different buckets.
library gcloud.storage;

import 'dart:async';

import 'package:http/http.dart' as http;

import 'package:crypto/crypto.dart' as crypto;
import 'package:googleapis/storage/v1.dart' as storage;
import 'package:googleapis/common/common.dart' as common;

import 'common.dart';
export 'common.dart';

part 'src/storage_impl.dart';

/// Bucket Access Control List
///
/// Describe an access control list for a bucket. The access control list
/// defines the level of access for different entities.
///
/// Currently only supports pre-defined ACLs.
///
/// TODO: Support for building custom ACLs.
class BucketAcl {
  static const AUTHENTICATED_READ = const BucketAcl._('authenticatedRead');
  static const PRIVATE = const BucketAcl._('private');
  static const PROJECT_PRIVATE = const BucketAcl._('projectPrivate');
  static const PUBLIC_READ = const BucketAcl._('publicRead');
  static const PUBLIC_READ_WRITE = const BucketAcl._('publicReadWrite');

  // Enum value for a predefined bucket ACL.
  final String _predefined;

  /// Whether this ACL is one of the predefined ones.
  bool get isPredefined => true;

  const BucketAcl._(String this._predefined);
}

/// Object Access Control List
///
/// Currently only supports pre-defined ACLs.
///
/// Describe an access control list for an object. The access control list
/// define the level of access for different entities.
///
/// TODO: Support for building custom ACLs.
class ObjectAcl {
  static const AUTHENTICATED_READ = const ObjectAcl._('authenticatedRead');
  static const BUCKET_OWNER_FULL_CONTROL =
      const ObjectAcl._('bucketOwnerFullControl');
  static const BUCKET_OWNER_READ = const ObjectAcl._('bucketOwnerRead');
  static const PRIVATE = const ObjectAcl._('private');
  static const PROJECT_PRIVATE = const ObjectAcl._('projectPrivate');
  static const PUBLIC_READ = const ObjectAcl._('publicRead');

  // Enum value for a predefined bucket ACL.
  final String _predefined;

  /// Whether this ACL is one of the predefined ones.
  bool get isPredefined => true;

  const ObjectAcl._(String this._predefined);
}

/// Information on a bucket.
abstract class BucketInfo {
  /// Name of the bucket.
  String get bucketName;

  /// When this bucket was created.
  DateTime get created;
}

/// Access to Cloud Storage
abstract class Storage {
  /// List of required OAuth2 scopes for Cloud Storage operation.
  static const Scopes = const [ storage.StorageApi.DevstorageFullControlScope ];

  /// Initializes access to cloud storage.
  factory Storage(http.Client client, String project) = _StorageImpl;

  /// Create a cloud storage bucket.
  ///
  /// Creates a cloud storage bucket named [bucketName].
  ///
  /// Returns a [Future] which completes when the bucket has been created.
  Future createBucket(String bucketName, {BucketAcl acl});

  /// Delete a cloud storage bucket.
  ///
  /// Deletes the cloud storage bucket named [bucketName].
  ///
  /// If the bucket is not empty the operation will fail.
  ///
  /// The returned [Future] completes when the operation is finished.
  Future deleteBucket(String bucketName);

  /// Access bucket object operations.
  ///
  /// Instantiates a `Bucket` object refering to the bucket named [bucketName].
  ///
  /// If the [defaultObjectAcl] argument is passed the resulting `Bucket` will
  /// attach this ACL to all objects created using this `Bucket` object.
  ///
  /// Otherwise the default object ACL attached to the bucket will be used.
  ///
  /// Returns a `Bucket` instance.
  Bucket bucket(String bucketName, {ObjectAcl defaultObjectAcl});

  /// Check whether a cloud storage bucket exists.
  ///
  /// Checks whether the bucket named [bucketName] exists.
  ///
  /// Returns a [Future] which completes with `true` if the bucket exists.
  Future<bool> bucketExists(String bucketName);

  /// Get information on a bucket
  ///
  /// Provide metadata information for bucket named [bucketName].
  ///
  /// Returns a [Future] which completes with a `BuckerInfo` object.
  Future<BucketInfo> bucketInfo(String bucketName);

  /// List names of all buckets.
  ///
  /// Returns a [Stream] of bucket names.
  Stream<String> listBucketNames();

  /// Start paging through names of all buckets.
  ///
  /// The maximum number of buckets in each page is specified in [pageSize].
  ///
  /// Returns a [Future] which completes with a `Page` object holding the
  /// first page. Use the `Page` object to move to the next page of buckets.
  Future<Page<String>> pageBucketNames({int pageSize: 50});

  /// Copy an object.
  ///
  /// Copy object [src] to object [dest].
  ///
  /// The names of [src] and [dest] must be absolute.
  Future copyObject(String src, String dest);
}

/// Information on a specific object.
///
/// This class provides access to information on an object. This includes
/// both the properties which are provided by Cloud Storage (such as the
/// MD5 hash) and the properties which can be changed (such as content type).
///
///  The properties provided by Cloud Storage are direct properties on this
///  object.
///
///  The mutable properties are properties on the `metadata` property.
abstract class ObjectInfo {
  /// Name of the object.
  String get name;

  /// Size of the data.
  int get size;

  /// When this object was updated.
  DateTime get updated;

  /// MD5 hash of the object.
  List<int> get md5Hash;

  /// CRC32c checksum, as described in RFC 4960.
  int get crc32CChecksum;

  /// URL for direct download.
  Uri get downloadLink;

  /// Object generation.
  ObjectGeneration get generation;

  /// Additional metadata.
  ObjectMetadata get metadata;
}

/// Generational information on an object.
abstract class ObjectGeneration {
  /// Object generation.
  String get objectGeneration;

  /// Metadata generation.
  int get metaGeneration;
}

/// Access to object metadata
abstract class ObjectMetadata {
  factory ObjectMetadata({ObjectAcl acl,
                          String contentType,
                          String contentEncoding,
                          String cacheControl,
                          String contentDisposition,
                          String contentLanguage,
                          Map<String, String> custom}) = _ObjectMetadata;
  /// ACL
  ///
  /// Currently it is only possible to set the ACL on one of the predefined
  /// values from the class `ObjectAcl`.
  void set acl(ObjectAcl value);

  /// `Content-Type` for this object.
  String contentType;

  /// `Content-Encoding` for this object.
  String contentEncoding;

  /// `Cache-Control` for this object.
  String cacheControl;

  /// `Content-Disposition` for this object.
  String contentDisposition;

  /// `Content-Language` for this object.
  ///
  /// The value of this field must confirm to RFC 3282.
  String contentLanguage;

  /// Custom metadata.
  Map<String, String> custom;

  /// Create a copy of this object with some values replaces.
  ///
  /// TODO: This cannot be used to set values to null.
  ObjectMetadata replace({ObjectAcl acl,
                          String contentType,
                          String contentEncoding,
                          String cacheControl,
                          String contentDisposition,
                          String contentLanguage,
                          Map<String, String> custom});
}

/// Result from List objects in a bucket.
///
/// Listing operate like a directory listing, despite the object
/// namespace being flat.
///
/// See [Bucket.list] for information on how the hierarchical structure
/// is determined.
class BucketEntry {
  /// Whether this is information on an object.
  final bool isObject;

  /// Name of object or directory.
  final String name;

  BucketEntry._object(this.name) : isObject = true;

  BucketEntry._directory(this.name) : isObject = false;

  /// Whether this is a prefix.
  bool get isDirectory => !isObject;
}

/// Access to operations on a specific cloud storage buket.
abstract class Bucket {
  /// Name of this bucket.
  String get bucketName;

  /// Absolute name of an object in this bucket. This includes the gs:// prefix.
  String absoluteObjectName(String objectName);

  /// Create a new object.
  ///
  /// Create an object named [objectName] in the bucket.
  ///
  /// If an object named [objectName] already exists this object will be
  /// replaced.
  ///
  /// If the length of the data to write is known in advance this can be passed
  /// as [length]. This can help to optimize the upload process.
  ///
  /// Additional metadata on the object can be passed either through the
  /// `metadata` argument or through the specific named arguments
  /// (such as `contentType`). Values passed through the specific named
  /// arguments takes precedence over the values in `metadata`.
  ///
  /// If [contentType] is not passed the default value of
  /// `application/octet-stream` will be used.
  ///
  /// Returns a `StreamSink` where the object content can be written. When
  /// The object content has been written the `StreamSink` completes with
  /// an `ObjectStat` instance with the information on the object created.
  StreamSink<List<int>> write(
      String objectName,
      {int length, ObjectMetadata metadata, String contentType});

  /// Create an new object in the bucket with specified content.
  ///
  /// Writes [bytes] to the created object.
  ///
  /// See [write] for more information on the additional arguments.
  ///
  /// Returns a `Future` which completes when the object is written.
  Future writeBytes(String name, List<int> bytes,
                    {String contentType, ObjectMetadata metadata});

  /// Read object content.
  ///
  /// TODO: More documentation
  Stream<List<int>> read(String objectName, {int offset: 0, int length});

  /// Lookup object metadata.
  ///
  /// TODO: More documentation
  Future<ObjectInfo> info(String name);

  /// Update object metadata.
  ///
  /// TODO: More documentation
  Future updateMetadata(String objectName, ObjectMetadata metadata);

  /// List objects in the bucket.
  ///
  /// Listing operates like a directory listing, despite the object
  /// namespace being flat. The character `/` is being used to separate
  /// object names into directory components.
  ///
  /// Retrieves a list of objects and directory components starting
  /// with [prefix].
  ///
  /// Returns a [Stream] of [BucketEntry]. Each element of the stream
  /// represents either an object or a directory component.
  Stream<BucketEntry> list({String prefix});

  /// Start paging through objects in the bucket.
  ///
  /// The maximum number of objects in each page is specified in [pageSize].
  ///
  /// See [list] for more information on the other arguments.
  ///
  /// Returns a `Future` which completes with a `Page` object holding the
  /// first page. Use the `Page` object to move to the next page.
  Future<Page<BucketEntry>> page({String prefix, int pageSize: 50});
}
