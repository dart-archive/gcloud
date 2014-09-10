// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of gcloud.pubsub;

class _PubSubImpl implements PubSub {
  final http.Client _client;
  final String project;
  final pubsub.PubsubApi _api;
  final String _topicPrefix;
  final String _subscriptionPrefix;

  _PubSubImpl(client, project) :
    this._client = client,
    this.project = project,
    _api = new pubsub.PubsubApi(client),
    _topicPrefix = '/topics/$project/',
    _subscriptionPrefix = '/subscriptions/$project/';


  String _fullTopicName(String name) {
    if (name.startsWith('/') && !name.startsWith('/topics')) {
      throw new ArgumentError("Illegal absolute topic name. Absolute topic "
                              "name must start with '/topics'");
    }
    return name.startsWith('/topics') ? name : '${_topicPrefix}$name';
  }

  String _fullSubscriptionName(name) {
      if (name.startsWith('/') && !name.startsWith('/subscriptions')) {
        throw new ArgumentError("Illegal absolute topic name. Absolute topic "
                                "name must start with '/subscriptions'");
      }
      return name.startsWith('/subscriptions') ? name
                                               : '${_subscriptionPrefix}$name';
  }

  Future<pubsub.Topic> _createTopic(String name) {
    return _api.topics.create(new pubsub.Topic()..name = name);
  }

  Future _deleteTopic(String name) {
    return _api.topics.delete(name);
  }

  Future<pubsub.Topic> _getTopic(String name) {
    return _api.topics.get(name);
  }

  Future<pubsub.ListTopicsResponse> _listTopics(
      int pageSize, String nextPageToken) {
    var query = 'cloud.googleapis.com/project in (/projects/$project)';
    return _api.topics.list(
        query: query, maxResults: pageSize, pageToken: nextPageToken);
  }

  Future<pubsub.Subscription> _createSubscription(
      String name, String topic, Uri endpoint) {
    var subscription = new pubsub.Subscription()
        ..name = name
        ..topic = topic;
    if (endpoint != null) {
      var pushConfig =
          new pubsub.PushConfig()..pushEndpoint = endpoint.toString();
      subscription.pushConfig = pushConfig;
    }
    return _api.subscriptions.create(subscription);
  }

  Future _deleteSubscription(String name) {
    return _api.subscriptions.delete(_fullSubscriptionName(name));
  }

  Future<pubsub.Subscription> _getSubscription(String name) {
    return _api.subscriptions.get(name);
  }

  Future<pubsub.ListSubscriptionsResponse> _listSubscriptions(
      String topic, int pageSize, String nextPageToken) {
    // See https://developers.google.com/pubsub/v1beta1/subscriptions/list for
    // the specification of the query format.
    var query = topic == null
        ? 'cloud.googleapis.com/project in (/projects/$project)'
        : 'pubsub.googleapis.com/topic in (/topics/$project/$topic)';
    return _api.subscriptions.list(
        query: query, maxResults: pageSize, pageToken: nextPageToken);
  }

  Future _modifyPushConfig(String subscription, Uri endpoint) {
    var pushConfig = new pubsub.PushConfig()
         ..pushEndpoint = endpoint != null ? endpoint.toString() : null;
    var request = new pubsub.ModifyPushConfigRequest()
        ..subscription = subscription
        ..pushConfig = pushConfig;
    return _api.subscriptions.modifyPushConfig(request);
  }

  Future _publish(
      String topic, List<int> message, Map<String, dynamic> labels) {
    var l = null;
    if (labels != null) {
      l = [];
      labels.forEach((key, value) {
        if (value is String) {
          l.add(new pubsub.Label()..key = key..strValue = value);
        } else {
          l.add(new pubsub.Label()..key = key..numValue = value.toString());
        }
      });
    }
    var request = new pubsub.PublishRequest()
        ..topic = topic
        ..message = (new pubsub.PubsubMessage()
            ..dataAsBytes = message
            ..label = l);
    return _api.topics.publish(request);
  }

  Future<pubsub.PullResponse> _pull(
      String subscription, bool returnImmediately) {
    var request = new pubsub.PullRequest()
        ..subscription = subscription
        ..returnImmediately = returnImmediately;
    return _api.subscriptions.pull(request);
  }

  Future _ack(String ackId, String subscription) {
    var request = new pubsub.AcknowledgeRequest()
        ..ackId = [ ackId ]
        ..subscription = subscription;
    return _api.subscriptions.acknowledge(request);
  }

  void _checkTopicName(name) {
    if (name.startsWith('/') && !name.startsWith(_topicPrefix)) {
      throw new ArgumentError(
          "Illegal topic name. Absolute topic names for project '$project' "
          "must start with $_topicPrefix");
    }
    if (name.length == _topicPrefix.length) {
      throw new ArgumentError(
          'Illegal topic name. Relative part of the name cannot be empty');
    }
  }

  void _checkSubscriptionName(name) {
    if (name.startsWith('/') && !name.startsWith(_subscriptionPrefix)) {
      throw new ArgumentError(
          "Illegal subscription name. Absolute subscription names for project "
          "'$project' must start with $_subscriptionPrefix");
    }
    if (name.length == _subscriptionPrefix.length) {
      throw new ArgumentError(
          'Illegal subscription name. '
          'Relative part of the name cannot be empty');
    }
  }

  Future<Topic> createTopic(String name) {
    _checkTopicName(name);
    return _createTopic(_fullTopicName(name))
        .then((top) => new _TopicImpl(this, top));
  }

  Future deleteTopic(String name) {
    _checkTopicName(name);
    return _deleteTopic(_fullTopicName(name));
  }

  Future<Topic> lookupTopic(String name) {
    _checkTopicName(name);
    return _getTopic(_fullTopicName(name))
        .then((top) => new _TopicImpl(this, top));
  }

  Stream<Topic> listTopics() {
    Future<Page<Topic>> firstPage(pageSize) {
      return _listTopics(pageSize, null)
        .then((response) => new _TopicPageImpl(this, pageSize, response));
    }
    return new StreamFromPages<Topic>(firstPage).stream;
  }

  Future<Page<Topic>> pageTopics({int pageSize: 50}) {
    return _listTopics(pageSize, null).then((response) {
      return new _TopicPageImpl(this, pageSize, response);
    });
  }

  Future<Subscription> createSubscription(
      String name, String topic, {Uri endpoint}) {
    _checkSubscriptionName(name);
    _checkTopicName(topic);
    return _createSubscription(_fullSubscriptionName(name),
                               _fullTopicName(topic),
                               endpoint)
        .then((sub) => new _SubscriptionImpl(this, sub));
  }

  Future deleteSubscription(String name) {
    _checkSubscriptionName(name);
    return _deleteSubscription(_fullSubscriptionName(name));
  }

  Future<Subscription> lookupSubscription(String name) {
    _checkSubscriptionName(name);
    return _getSubscription(_fullSubscriptionName(name))
        .then((sub) => new _SubscriptionImpl(this, sub));
  }

  Stream<Subscription> listSubscriptions([String query]) {
    Future<Page<Subscription>> firstPage(pageSize) {
      return _listSubscriptions(query, pageSize, null)
        .then((response) =>
            new _SubscriptionPageImpl(this, query, pageSize, response));
    }
    return new StreamFromPages<Subscription>(firstPage).stream;
  }

  Future<Page<Subscription>> pageSubscriptions(
      {String topic, int pageSize: 50}) {
    return _listSubscriptions(topic, pageSize, null).then((response) {
      return new _SubscriptionPageImpl(this, topic, pageSize, response);
    });
  }
}

/// Message class for messages constructed through 'new Message()'. It stores
/// the user supplied body as either String or bytes.
class _MessageImpl implements Message {
  // The message body, if it is a `String`. In that case, [bytesMessage] is
  // null.
  final String _stringMessage;

  // The message body, if it is a byte list. In that case, [stringMessage] is
  // null.
  final List<int> _bytesMessage;

  final Map labels;

  _MessageImpl.withString(this._stringMessage, {this.labels})
      : _bytesMessage = null;

  _MessageImpl.withBytes(this._bytesMessage, {this.labels})
      : _stringMessage = null;

  List<int> get asBytes =>
      _bytesMessage != null ? _bytesMessage : UTF8.encode(_stringMessage);

  String get asString =>
      _stringMessage != null ? _stringMessage : UTF8.decode(_bytesMessage);
}

/// Message received using [Subscription.pull].
///
/// Contains the [pubsub.PubsubMessage] received from Pub/Sub, and
/// makes the message body and labels available on request.
///
/// The labels map is lazily created when first accessed.
class _PullMessage implements Message {
  final pubsub.PubsubMessage _message;
  List<int> _bytes;
  String _string;
  Map _labels;

  _PullMessage(this._message);

  List<int> get asBytes {
    if (_bytes == null) _bytes = _message.dataAsBytes;
    return _bytes;
  }

  String get asString {
    if (_string == null) _string = UTF8.decode(_message.dataAsBytes);
    return _string;
  }

  Map<String, dynamic> get labels {
    if (_labels == null) {
      _labels = <String, dynamic>{};
      _message.label.forEach((label) {
        _labels[label.key] =
            label.numValue != null ? label.numValue : label.strValue;
      });
    }
    return _labels;
  }
}

/// Message received through Pub/Sub push delivery.
///
/// Stores the message body received from Pub/Sub as the Base64 encoded string
/// from the wire protocol.
///
/// The labels have been decoded into a Map.
class _PushMessage implements Message {
  final String _base64Message;
  final Map labels;

  _PushMessage(this._base64Message, this.labels);

  List<int> get asBytes => CryptoUtils.base64StringToBytes(_base64Message);

  String get asString => UTF8.decode(asBytes);
}

/// Pull event received from Pub/Sub pull delivery.
///
/// Stores the pull response received from Pub/Sub.
class _PullEventImpl implements PullEvent {
  /// Pub/Sub API object.
  final _PubSubImpl _api;
  /// Low level response received from Pub/Sub.
  final pubsub.PullResponse _response;
  final Message message;

  _PullEventImpl(this._api, response)
      : this._response = response,
        message = new _PullMessage(response.pubsubEvent.message);

  bool get isTruncated => _response.pubsubEvent.truncated;

  Future acknowledge() {
    return _api._ack(_response.ackId, _response.pubsubEvent.subscription);
  }

}

/// Push event received from Pub/Sub push delivery.
///
/// decoded from JSON encoded push HTTP request body.
class _PushEventImpl implements PushEvent {
  static const PREFIX = '/subscriptions/';
  final Message _message;
  final String _subscriptionName;

  Message get message => _message;

  String get subscriptionName => _subscriptionName;

  _PushEventImpl(this._message, this._subscriptionName);

  factory _PushEventImpl.fromJson(String json) {
    Map body = JSON.decode(json);
    String data = body['message']['data'];
    Map labels = new HashMap();
    body['message']['labels'].forEach((label) {
      var key = label['key'];
      var value = label['strValue'];
      if (value == null) value = label['numValue'];
      labels[key] = value;
    });
    String subscription = body['subscription'];
    // TODO(#1): Remove this when the push event subscription name is prefixed
    // with '/subscriptions/'.
    if (!subscription.startsWith(PREFIX)) {
      subscription = PREFIX + subscription;
    }
    return new _PushEventImpl(new _PushMessage(data, labels), subscription);
  }
}

class _TopicImpl implements Topic {
  final _PubSubImpl _api;
  final pubsub.Topic _topic;

  _TopicImpl(this._api, this._topic);

  String get name {
    assert(_topic.name.startsWith(_api._topicPrefix));
    return _topic.name.substring(_api._topicPrefix.length);
  }

  String get project {
    assert(_topic.name.startsWith(_api._topicPrefix));
    return _api.project;
  }

  String get absoluteName => _topic.name;

  Future publish(Message message) {
    return _api._publish(_topic.name, message.asBytes, message.labels);
  }

  Future delete() => _api._deleteTopic(_topic.name);

  Future publishString(String message, {Map<String, dynamic> labels}) {
    return _api._publish(_topic.name, UTF8.encode(message), labels);
  }

  Future publishBytes(List<int> message, {Map<String, dynamic> labels}) {
    return _api._publish(_topic.name, message, labels);
  }
}

class _SubscriptionImpl implements Subscription {
  final _PubSubImpl _api;
  final pubsub.Subscription _subscription;

  _SubscriptionImpl(this._api, this._subscription);

  String get name {
    assert(_subscription.name.startsWith(_api._subscriptionPrefix));
    return _subscription.name.substring(_api._subscriptionPrefix.length);
  }

  String get project {
    assert(_subscription.name.startsWith(_api._subscriptionPrefix));
    return _api.project;
  }

  String get absoluteName => _subscription.name;

  Topic get topic {
    var topic = new pubsub.Topic()..name = _subscription.topic;
    return new _TopicImpl(_api, topic);
  }

  Future delete() => _api._deleteSubscription(_subscription.name);

  Future<PullEvent> pull({bool noWait: true}) {
    return _api._pull(_subscription.name, noWait)
        .then((response) {
          return new _PullEventImpl(_api, response);
        }).catchError((e) => null,
                      test: (e) => e is pubsub.DetailedApiRequestError &&
                                   e.status == 400);
  }

  Uri get endpoint => null;

  bool get isPull => endpoint == null;

  bool get isPush => endpoint != null;

  Future updatePushConfiguration(Uri endpoint) {
    return _api._modifyPushConfig(_subscription.name, endpoint);
  }
}

class _TopicPageImpl implements Page<Topic> {
  final _PubSubImpl _api;
  final int _pageSize;
  final String _nextPageToken;
  final List<Topic> items;

  _TopicPageImpl(this._api,
                this._pageSize,
                pubsub.ListTopicsResponse response)
      : items = new List(response.topic.length),
        _nextPageToken = response.nextPageToken {
    for (int i = 0; i < response.topic.length; i++) {
      items[i] = new _TopicImpl(_api, response.topic[i]);
    }
  }

  bool get isLast => _nextPageToken == null;

  Future<Page<Topic>> next({int pageSize}) {
    if (isLast) return new Future.value(null);
    if (pageSize == null) pageSize = this._pageSize;

    return _api._listTopics(pageSize, _nextPageToken).then((response) {
      return new _TopicPageImpl(_api, pageSize, response);
    });
  }
}

class _SubscriptionPageImpl implements Page<Subscription> {
  final _PubSubImpl _api;
  final String _topic;
  final int _pageSize;
  final String _nextPageToken;
  final List<Subscription> items;

  _SubscriptionPageImpl(this._api,
                        this._topic,
                        this._pageSize,
                        pubsub.ListSubscriptionsResponse response)
      : items = new List(response.subscription != null
                                               ? response.subscription.length
                                               : 0),
        _nextPageToken = response.nextPageToken{
    if (response.subscription != null) {
      for (int i = 0; i < response.subscription.length; i++) {
        items[i] = new _SubscriptionImpl(_api, response.subscription[i]);
      }
    }
  }

  bool get isLast => _nextPageToken == null;

  Future<Page<Subscription>> next({int pageSize}) {
    if (_nextPageToken == null) return new Future.value(null);
    if (pageSize == null) pageSize = this._pageSize;

    return _api._listSubscriptions(
        _topic, pageSize, _nextPageToken).then((response) {
      return new _SubscriptionPageImpl(_api, _topic, pageSize, response);
    });
  }
}