// Copyright (c) 2015, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';

import 'package:gcloud/pubsub.dart';
import 'package:googleapis/common/common.dart' as common;
import 'package:unittest/unittest.dart';

import '../common_e2e.dart';

String generateTopicName() {
  var id = new DateTime.now().millisecondsSinceEpoch;
  return 'dart-e2e-test-$id';
}

runTests(PubSub pubsub, String project) {
  group('topic', () {
    test('create-lookup-delete', () async {
      var topicName = generateTopicName();
      var topic = await pubsub.createTopic(topicName);
      expect(topic.name, topicName);
      topic = await pubsub.lookupTopic(topicName);
      expect(topic.name, topicName);
      expect(topic.project, project);
      expect(topic.absoluteName, 'projects/$project/topics/$topicName');
      expect(await pubsub.deleteTopic(topicName), isNull);
    });

    test('create-list-delete', () async {
      var topicPrefix = generateTopicName();

      name(i) => '$topicPrefix-$i';

      for (var i = 0; i < 5; i++) {
        await pubsub.createTopic(name(i));
      }
      var topics = await pubsub.listTopics().map((t) => t.name).toList();
      for (var i = 0; i < 5; i++) {
        expect(topics.contains(name(i)), isTrue);
        await pubsub.deleteTopic(name(i));
      }
    });
  });
}

main() {
  withAuthClient(PubSub.SCOPES, (String project, httpClient) {
    // Share the same pubsub connection for all tests.
    var pubsub = new PubSub(httpClient, project);

    return runE2EUnittest(() {
      runTests(pubsub, project);
    }).whenComplete(() {
      // TODO(sgjesse): Cleanup leftover topics/subscriptions.
    });
  });
}
