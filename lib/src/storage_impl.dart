// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of gcloud.storage;

const String _ABSOLUTE_PREFIX = 'gs://';
const String _DIRECTORY_DELIMITER = 'gs://';

/// Representation of an absolute name consisting of bucket name and object
/// name.
class _AbsoluteName {
  String bucketName;
  String objectName;

  _AbsoluteName.parse(String absoluteName) {
    if (!absoluteName.startsWith(_ABSOLUTE_PREFIX)) {
      throw new FormatException("Absolute name '$absoluteName' does not start "
                                "with '$_ABSOLUTE_PREFIX'");
    }
    int index = absoluteName.indexOf('/', _ABSOLUTE_PREFIX.length);
    if (index == -1 || index == _ABSOLUTE_PREFIX.length) {
      throw new FormatException("Absolute name '$absoluteName' does not have "
                                "a bucket name");
    }
    if (index == absoluteName.length - 1) {
      throw new FormatException("Absolute name '$absoluteName' does not have "
                                "an object name");
    }
    bucketName = absoluteName.substring(_ABSOLUTE_PREFIX.length, index);
    objectName = absoluteName.substring(index + 1);
  }
}

/// Storage API implementation providing access to buckets.
class _StorageImpl implements Storage {
  final String project;
  final storage.StorageApi _api;

  _StorageImpl(client, this.project)
      : _api = new storage.StorageApi(client);

  Future createBucket(String bucketName, {BucketAcl acl}) {
    var bucket = new storage.Bucket()..name = bucketName;
    var predefinedAcl;
    if (acl != null) {
      assert(acl.isPredefined);
      predefinedAcl = acl._predefined;
    }
    return _api.buckets.insert(bucket,
                               project,
                               predefinedAcl: predefinedAcl)
        .then((bucket) => null);
  }

  Future deleteBucket(String bucketName) {
    return _api.buckets.delete(bucketName);
  }

  Bucket bucket(String bucketName, {ObjectAcl defaultObjectAcl}) {
    return new _BucketImpl(this, bucketName, defaultObjectAcl);
  }

  Future<bool> bucketExists(String bucketName) {
    notFoundError(e) => e is common.DetailedApiRequestError && e.status == 404;
    return _api.buckets.get(bucketName)
        .then((_) => true)
        .catchError((e) => false, test: notFoundError);

  }

  Future<BucketInfo> bucketInfo(String bucketName) {
    return _api.buckets.get(bucketName)
        .then((bucket) => new _BucketInformationImpl(bucket));
  }

  Stream<String> listBucketNames() {
    Future<Page<Bucket>> firstPage(pageSize) {
      return _listBuckets(pageSize, null)
          .then((response) => new _BucketPageImpl(this, pageSize, response));
    }
    return new StreamFromPages<String>(firstPage).stream;
  }

  Future<Page<String>> pageBucketNames({int pageSize: 50}) {
    return _listBuckets(pageSize, null).then((response) {
      return new _BucketPageImpl(this, pageSize, response);
    });
  }

  Future copyObject(String src, String dest) {
    var srcName = new _AbsoluteName.parse(src);
    var destName = new _AbsoluteName.parse(dest);
    return _api.objects.copy(null,
                             srcName.bucketName, srcName.objectName,
                             destName.bucketName, destName.objectName)
        .then((_) => null);
  }

  Future<storage.Buckets> _listBuckets(int pageSize, String nextPageToken) {
    return _api.buckets.list(
        project,
        maxResults: pageSize,
        pageToken: nextPageToken);
  }
}

class _BucketInformationImpl implements BucketInfo {
  storage.Bucket _bucket;

  _BucketInformationImpl(this._bucket);

  String get bucketName => _bucket.name;

  DateTime get created => _bucket.timeCreated;
}

/// Bucket API implementation providing access to objects.
class _BucketImpl implements Bucket {
  final storage.StorageApi _api;
  ObjectAcl _defaultObjectAcl;
  final String bucketName;

  _BucketImpl(_StorageImpl storage, this.bucketName, this._defaultObjectAcl) :
    this._api = storage._api;

  String absoluteObjectName(String objectName) {
    return '${_ABSOLUTE_PREFIX}$bucketName/$objectName';
  }

  StreamSink<List<int>> write(
      String objectName,
      {int length, ObjectMetadata metadata, String contentType}) {
    storage.Object object;
    if (metadata == null) {
      metadata = new _ObjectMetadata(contentType: contentType);
    } else if (contentType != null) {
      metadata = metadata.replace(contentType: contentType);
    }
    object = (metadata as _ObjectMetadata)._object;

    // Fill properties not passed in metadata.
    object.name = objectName;

    var sink = new _MediaUploadStreamSink(
        _api, bucketName, objectName, object, length);
    return sink;
  }

  Future writeBytes(
      String objectName, List<int> bytes,
      {ObjectMetadata metadata, String contentType}) {
    var sink = write(objectName, length: bytes.length,
                     metadata: metadata, contentType: contentType);
    sink.add(bytes);
    return sink.close();
  }

  Stream read(String objectName, {int offset: 0, int length}) {
    var controller = new StreamController();
    _api.objects.get(
        bucketName,
        objectName,
        downloadOptions: common.DownloadOptions.FullMedia).then(
        (media) => media.stream.pipe(controller.sink));
    return controller.stream;
  }

  Future<ObjectInfo> info(String objectName) {
    return _api.objects.get(bucketName, objectName)
        .then((object) => new _ObjectStatImpl(object));
  }

  Stream<BucketEntry> list({String prefix}) {
    Future<Page<Bucket>> firstPage(pageSize) {
      return _listObjects(bucketName, prefix, _DIRECTORY_DELIMITER, 50, null)
          .then((response) => new _ObjectPageImpl(
              this, prefix, pageSize, response));
    }
    return new StreamFromPages<BucketEntry>(firstPage).stream;
  }

  Future<Page<BucketEntry>> page({String prefix, int pageSize: 50}) {
    return _listObjects(
        bucketName, prefix, _DIRECTORY_DELIMITER, pageSize, null)
        .then((response) {
          return new _ObjectPageImpl(this, prefix, pageSize, response);
        });
  }

  Future updateMetadata(String objectName, ObjectMetadata metadata) {
    // TODO: support other ObjectMetadata implementations?
    _ObjectMetadata md = metadata;
    var object = md._object;
    if (md._predefined == null && _defaultObjectAcl == null) {
      throw new ArgumentError('ACL is required for update');
    }
    if (md.contentType == null) {
      throw new ArgumentError('Content-Type is required for update');
    }
    var acl = md._predefined != null ? md._predefined._predefined
                                     : _defaultObjectAcl._predefined;
    return _api.objects.update(
        object, bucketName, objectName, predefinedAcl: acl);
  }

  Future<storage.Objects> _listObjects(
      String bucketName, String prefix, String delimiter,
      int pageSize, String nextPageToken) {
    return _api.objects.list(
        bucketName,
        prefix: prefix,
        delimiter: delimiter,
        maxResults: pageSize,
        pageToken: nextPageToken);
  }
}

class _BucketPageImpl implements Page<String> {
  final _StorageImpl _storage;
  final int _pageSize;
  final String _nextPageToken;
  final List<String> items;

  _BucketPageImpl(this._storage, this._pageSize, storage.Buckets response)
      : items = new List(response.items != null ? response.items.length : 0),
        _nextPageToken = response.nextPageToken {
    for (int i = 0; i < items.length; i++) {
      items[i] = response.items[i].name;
    }
  }

  bool get isLast => _nextPageToken == null;

  Future<Page<String>> next({int pageSize}) {
    if (isLast) return new Future.value(null);
    if (pageSize == null) pageSize = this._pageSize;

    return _storage._listBuckets(pageSize, _nextPageToken).then((response) {
      return new _BucketPageImpl(_storage, pageSize, response);
    });
  }
}

class _ObjectPageImpl implements Page<BucketEntry> {
  final _BucketImpl _bucket;
  final String _prefix;
  final int _pageSize;
  final String _nextPageToken;
  final List<BucketEntry> items;

  _ObjectPageImpl(
      this._bucket, this._prefix, this._pageSize,
      storage.Objects response)
      : items = new List(
            (response.items != null ? response.items.length : 0) +
            (response.prefixes != null ? response.prefixes.length : 0)),
        _nextPageToken = response.nextPageToken {
    var prefixes = 0;
    if (response.prefixes != null) {
      for (int i = 0; i < response.prefixes.length; i++) {
        items[i] = new BucketEntry._directory(response.prefixes[i]);
      }
      prefixes = response.prefixes.length;
    }
    if (response.items != null) {
      for (int i = 0; i < response.items.length; i++) {
        items[prefixes + i] = new BucketEntry._object(response.items[i].name);
      }
    }
  }

  bool get isLast => _nextPageToken == null;

  Future<Page<BucketEntry>> next({int pageSize}) {
    if (isLast) return new Future.value(null);
    if (pageSize == null) pageSize = this._pageSize;

    return _bucket._listObjects(
        _bucket.bucketName,
        _prefix,
        _DIRECTORY_DELIMITER,
        pageSize,
        _nextPageToken).then((response) {
      return new _ObjectPageImpl(
          _bucket, _prefix, pageSize, response);
    });
  }
}

class _ObjectGenerationImpl implements ObjectGeneration {
  final String objectGeneration;
  final int metaGeneration;

  _ObjectGenerationImpl(this.objectGeneration, this.metaGeneration);
}

class _ObjectStatImpl implements ObjectInfo {
  storage.Object _object;
  Uri _downloadLink;
  ObjectGeneration _generation;
  ObjectMetadata _metadata;

  _ObjectStatImpl(object) :
      _object = object, _metadata = new _ObjectMetadata._(object);

  String get name => _object.name;

  int get size => int.parse(_object.size);

  DateTime get updated  => _object.updated;

  List<int> get md5Hash =>
      crypto.CryptoUtils.base64StringToBytes(_object.md5Hash);

  int get crc32CChecksum  => int.parse(_object.crc32c);

  Uri get downloadLink {
    if (_downloadLink == null) {
      _downloadLink = Uri.parse(_object.mediaLink);
    }
    return _downloadLink;
  }

  ObjectGeneration get generation {
    if (_generation == null) {
      _generation = new _ObjectGenerationImpl(
          _object.generation, int.parse(_object.metageneration));
    }
    return _generation;
  }

  /// Additional metadata.
  ObjectMetadata get metadata => _metadata;
}

class _ObjectMetadata implements ObjectMetadata {
  storage.Object _object;
  ObjectAcl _predefined;

  _ObjectMetadata({ObjectAcl acl,
                   String contentType,
                   String contentEncoding,
                   String cacheControl,
                   String contentDisposition,
                   String contentLanguage,
                   Map<String, String> custom}) {
    _object = new storage.Object();
    _predefined = acl;  // Only canned ACLs supported.
    _object.contentType = contentType;
    _object.contentEncoding = contentEncoding;
    _object.cacheControl = cacheControl;
    _object.contentDisposition = contentDisposition;
    _object.contentLanguage = contentLanguage;
    if (custom != null) _object.metadata = custom;
  }

  _ObjectMetadata._(this._object);

  set acl(ObjectAcl value) => _predefined = value;

  String get contentType => _object.contentType;
  set contentType(String value) => _object.contentType = value;

  String get contentEncoding => _object.contentEncoding;
  set contentEncoding(String value) => _object.contentEncoding = value;

  String get cacheControl => _object.cacheControl;
  set cacheControl(String value) => _object.cacheControl = value;

  String get contentDisposition => _object.contentDisposition;
  set contentDisposition(String value) => _object.contentDisposition = value;

  String get contentLanguage => _object.contentLanguage;
  set contentLanguage(String value) => _object.contentLanguage = value;

  Map<String, String> get custom => _object.metadata;
  set custom(Map<String, String> value) => _object.metadata = value;

  ObjectMetadata replace({ObjectAcl acl,
                          String contentType,
                          String contentEncoding,
                          String cacheControl,
                          String contentDisposition,
                          String contentLanguage,
                          Map<String, String> custom}) {
    return new _ObjectMetadata(
        acl: acl != null ? acl : _predefined,
        contentType: contentType != null ? contentType : this.contentType,
        contentEncoding: contentEncoding != null ? contentEncoding
                                                 : this.contentEncoding,
        cacheControl: cacheControl != null ? cacheControl : this.cacheControl,
        contentDisposition: contentDisposition != null ? contentDisposition
                                                       : this.contentEncoding,
        contentLanguage: contentLanguage != null ? contentLanguage
                                                 : this.contentEncoding,
        custom: custom != null ? custom : this.custom);
  }
}

/// Implementation of StreamSink which handles Google media upload.
/// It provides a StreamSink and logic which selects whether to use normal
/// media upload (multipart mime) or resumable media upload.
class _MediaUploadStreamSink implements StreamSink<List<int>> {
  static const int _DEFAULT_MAX_NORMAL_UPLOAD_LENGTH = 1024 * 1024;
  final storage.StorageApi _api;
  final String _bucketName;
  final String _objectName;
  final storage.Object _object;
  final int _length;
  final int _maxNormalUploadLength;
  int _bufferLength = 0;
  final List<List<int>> buffer = new List<List<int>>();
  final StreamController _controller = new StreamController(sync: true);
  StreamSubscription _subscription;
  StreamController _resumableController;
  final _doneCompleter = new Completer();

  static const int _STATE_LENGTH_KNOWN = 0;
  static const int _STATE_PROBING_LENGTH = 1;
  static const int _STATE_DECIDED_RESUMABLE = 2;
  int _state;

  _MediaUploadStreamSink(
      this._api, this._bucketName, this._objectName, this._object, this._length,
      [this._maxNormalUploadLength = _DEFAULT_MAX_NORMAL_UPLOAD_LENGTH]) {
    if (_length != null) {
      // If the length is known in advance decide on the upload strategy
      // immediately
      _state = _STATE_LENGTH_KNOWN;
      if (_length <= _maxNormalUploadLength) {
        _startNormalUpload(_controller.stream, _length);
      } else {
        _startResumableUpload(_controller.stream, _length);
      }
    } else {
      _state = _STATE_PROBING_LENGTH;
      // If the length is not known in advance decide on the upload strategy
      // later. Start buffering until enough data has been read to decide.
      _subscription = _controller.stream.listen(
          _onData, onDone: _onDone, onError: _onError);
    }
  }

  void add(List<int> event) {
    _controller.add(event);
  }

  void addError(errorEvent, [StackTrace stackTrace]) {
    _controller.addError(errorEvent, stackTrace);
  }

  Future addStream(Stream<List<int>> stream) {
    return _controller.addStream(stream);
  }

  Future close() {
    _controller.close();
    return _doneCompleter.future;
  }

  Future get done => _doneCompleter.future;

  _onData(List<int> data) {
    assert(_state != _STATE_LENGTH_KNOWN);
    if (_state == _STATE_PROBING_LENGTH) {
      buffer.add(data);
      _bufferLength += data.length;
      if (_bufferLength > _maxNormalUploadLength) {
        // Start resumable upload.
        // TODO: Avoid using another stream-controller.
        _resumableController = new StreamController(sync: true);
        buffer.forEach(_resumableController.add);
        var media = new common.Media(_resumableController.stream, null);
        _startResumableUpload(_resumableController.stream, _length);
        _state = _STATE_DECIDED_RESUMABLE;
      }
    } else {
      assert(_state == _STATE_DECIDED_RESUMABLE);
      _resumableController.add(data);
    }
  }

  _onDone() {
    if (_state == _STATE_PROBING_LENGTH) {
      // As the data is already cached don't bother to wait on somebody
      // listening on the stream before adding the data.
      var controller = new StreamController();
      buffer.forEach(controller.add);
      controller.close();
      _startNormalUpload(controller.stream, _bufferLength);
    } else {
      _resumableController.close();
    }
  }

  _onError(e, s) {
    // If still deciding on the strategy complete with error. Otherwise
    // forward the error for default processing.
    if (_state == _STATE_PROBING_LENGTH) {
      _completeError(e, s);
    } else {
      _resumableController.addError(e, s);
    }
  }

  _completeError(e, s) {
    if (_state != _STATE_LENGTH_KNOWN) {
      // Always cancel subscription on error.
      _subscription.cancel();
    }
    _doneCompleter.completeError(e, s);
  }

  void _startNormalUpload(Stream stream, int length) {
    var media = new common.Media(stream, length);
    _api.objects.insert(_object,
                        _bucketName,
                        name: _objectName,
                        uploadMedia: media,
                        uploadOptions: common.UploadOptions.Default)
        .then((response) {
          _doneCompleter.complete(new _ObjectStatImpl(response));
        }, onError: _completeError);
  }

  void _startResumableUpload(Stream stream, int length) {
    var media = new common.Media(stream, length);
    _api.objects.insert(_object,
                        _bucketName,
                        name: _objectName,
                        uploadMedia: media,
                        uploadOptions: common.UploadOptions.Resumable)
        .then((response) {
          _doneCompleter.complete(new _ObjectStatImpl(response));
        }, onError: _completeError);
  }
}
