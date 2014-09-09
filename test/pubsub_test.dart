import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart' as http_testing;
import 'package:unittest/unittest.dart';

import 'package:gcloud/pubsub.dart';

import 'package:googleapis_beta/pubsub/v1beta1.dart' as pubsub;

const PROJECT = 'test-project';
const CONTENT_TYPE_JSON_UTF8 = 'application/json; charset=utf-8';
const RESPONSE_HEADERS = const {
  'content-type': CONTENT_TYPE_JSON_UTF8
};

const String ROOT_PATH = '/pubsub/v1beta1/';
final Uri ROOT_URI = Uri.parse('https://www.googleapis.com$ROOT_PATH');

class MockClient extends http.BaseClient {
  Map<String, Map<Pattern, Function>> mocks = {};
  http_testing.MockClient client;

  MockClient() {
    client = new http_testing.MockClient(handler);
  }

  void register(String method, Pattern path,
      http_testing.MockClientHandler handler) {
    mocks.putIfAbsent(method, () => new Map())[path] = handler;
  }

  void clear() {
    mocks = {};
  }

  Future<http.Response> handler(http.Request request) {
    expect(request.url.host, 'www.googleapis.com');
    expect(request.url.path.startsWith(ROOT_PATH), isTrue);
    var path = request.url.path.substring(ROOT_PATH.length);
    if (mocks[request.method] == null) {
      throw 'No mock handler for method ${request.method} found. '
            'Request URL was: ${request.url}';
    }
    var mockHandler;
    mocks[request.method].forEach((pattern, handler) {
      if (pattern.matchAsPrefix(path) != null) {
        mockHandler = handler;
      }
    });
    if (mockHandler == null) {
      throw 'No mock handler for method ${request.method} and path '
            '[$path] found. Request URL was: ${request.url}';
    }
    return mockHandler(request);
  }

  Future<http.StreamedResponse> send(http.BaseRequest request) {
    return client.send(request);
  }

  Future<http.Response> respond(response) {
    return new Future.value(
        new http.Response(
            JSON.encode(response.toJson()), 200, headers: RESPONSE_HEADERS));
  }

  Future<http.Response> respondEmpty() {
    return new Future.value(
        new http.Response('', 200, headers: RESPONSE_HEADERS));
  }

  Future<http.Response> respondError(statusCode) {
    var error = {'error' : {'code': statusCode, 'message': 'error'}};
    return new Future.value(
        new http.Response(
            JSON.encode(error), statusCode, headers: RESPONSE_HEADERS));
  }
}

main() {
  group('api', () {
    var badTopicNames = [
        '/', '/topics', '/topics/$PROJECT', '/topics/$PROJECT/',
        '/topics/${PROJECT}x', '/topics/${PROJECT}x/'];

    var badSubscriptionNames = [
        '/', '/subscriptions', '/subscriptions/$PROJECT',
        '/subscriptions/$PROJECT/', '/subscriptions/${PROJECT}x',
        '/subscriptions/${PROJECT}x/'];

    group('topic', () {
      var name = 'test-topic';
      var absoluteName = '/topics/$PROJECT/test-topic';

      test('create', () {
        var mock = new MockClient();
        mock.register('POST', 'topics', expectAsync((request) {
          var requestTopic =
              new pubsub.Topic.fromJson(JSON.decode(request.body));
          expect(requestTopic.name, absoluteName);
          return mock.respond(new pubsub.Topic()..name = absoluteName);
        }, count: 2));

        var api = new PubSub(mock, PROJECT);
        return api.createTopic(name).then(expectAsync((topic) {
          expect(topic.name, name);
          expect(topic.project, PROJECT);
          expect(topic.absoluteName, absoluteName);
          return api.createTopic(absoluteName).then(expectAsync((topic) {
            expect(topic.name, name);
            expect(topic.absoluteName, absoluteName);
          }));
        }));
      });

      test('create-error', () {
        var mock = new MockClient();
        var api = new PubSub(mock, PROJECT);
        badTopicNames.forEach((name) {
          expect(() => api.createTopic(name), throwsArgumentError);
        });
        badSubscriptionNames.forEach((name) {
          expect(() => api.createTopic(name), throwsArgumentError);
        });
      });

      test('delete', () {
        var mock = new MockClient();
        mock.register(
            'DELETE', new RegExp(r'topics/[a-z/-]*$'), expectAsync((request) {
          expect(request.url.path, '${ROOT_PATH}topics/$absoluteName');
          expect(request.body.length, 0);
          return mock.respondEmpty();
        }, count: 2));

        var api = new PubSub(mock, PROJECT);
        return api.deleteTopic(name).then(expectAsync((result) {
          expect(result, isNull);
          return api.deleteTopic(absoluteName).then(expectAsync((topic) {
            expect(result, isNull);
          }));
        }));
      });

      test('delete-error', () {
        var mock = new MockClient();
        var api = new PubSub(mock, PROJECT);
        badTopicNames.forEach((name) {
          expect(() => api.deleteTopic(name), throwsArgumentError);
        });
        badSubscriptionNames.forEach((name) {
          expect(() => api.deleteTopic(name), throwsArgumentError);
        });
      });

      test('lookup', () {
        var mock = new MockClient();
        mock.register(
            'GET', new RegExp(r'topics/[a-z/-]*$'), expectAsync((request) {
          expect(request.url.path, '${ROOT_PATH}topics/$absoluteName');
          expect(request.body.length, 0);
          return mock.respond(new pubsub.Topic()..name = absoluteName);
        }, count: 2));

        var api = new PubSub(mock, PROJECT);
        return api.lookupTopic(name).then(expectAsync((topic) {
          expect(topic.name, name);
          expect(topic.project, PROJECT);
          expect(topic.absoluteName, absoluteName);
          return api.lookupTopic(absoluteName).then(expectAsync((topic) {
            expect(topic.name, name);
            expect(topic.absoluteName, absoluteName);
          }));
        }));
      });

      test('lookup-error', () {
        var mock = new MockClient();
        var api = new PubSub(mock, PROJECT);
        badTopicNames.forEach((name) {
          expect(() => api.lookupTopic(name), throwsArgumentError);
        });
        badSubscriptionNames.forEach((name) {
          expect(() => api.lookupTopic(name), throwsArgumentError);
        });
      });

      group('query', () {
        var query = 'cloud.googleapis.com/project in (/projects/$PROJECT)';
        var defaultPageSize = 50;

        addTopics(pubsub.ListTopicsResponse response, int first, int count) {
          response.topic = [];
          for (int i = 0; i < count; i++) {
            response.topic.add(new pubsub.Topic()..name = 'topic-${first + i}');
          }
        }

        // Mock that expect/generates [n] topics in pages of page size
        // [pageSize].
        registerQueryMock(mock, n, pageSize, [totalCalls]) {
          var totalPages = (n + pageSize - 1) ~/ pageSize;
          // No items still generate one request.
          if (totalPages == 0) totalPages = 1;
          // Can pass in total calls if this mock is overwritten before all
          // expected pages are done, e.g. when testing errors.
          if (totalCalls == null) {
            totalCalls = totalPages;
          }
          var pageCount = 0;
          mock.register('GET', 'topics', expectAsync((request) {
            pageCount++;
            expect(request.url.queryParameters['query'], query);
            expect(request.url.queryParameters['maxResults'], '$pageSize');
            expect(request.body.length, 0);
            if (pageCount > 1) {
              expect(request.url.queryParameters['pageToken'], 'next-page');
            }

            var response = new pubsub.ListTopicsResponse();
            var first = (pageCount - 1) * pageSize + 1;
            if (pageCount < totalPages) {
              response.nextPageToken = 'next-page';
              addTopics(response, first, pageSize);
            } else {
              addTopics(response, first, n - (totalPages - 1) * pageSize);
            }
            return mock.respond(response);
          }, count: totalCalls));
        }

        group('list', () {
          Future q(count) {
            var mock = new MockClient();
            registerQueryMock(mock, count, 50);

            var api = new PubSub(mock, PROJECT);
            return api.listTopics().listen(
                expectAsync((_) => null, count: count)).asFuture();
          }

          test('simple', () {
            return q(0)
                .then((_) => q(1))
                .then((_) => q(1))
                .then((_) => q(49))
                .then((_) => q(50))
                .then((_) => q(51))
                .then((_) => q(99))
                .then((_) => q(100))
                .then((_) => q(101))
                .then((_) => q(170));
          });

          test('immediate-pause-resume', () {
            var mock = new MockClient();
            registerQueryMock(mock, 70, 50);

            var api = new PubSub(mock, PROJECT);
            api.listTopics().listen(
                expectAsync(((_) => null), count: 70),
                onDone: expectAsync(() => null))
                    ..pause()
                    ..resume()
                    ..pause()
                    ..resume();
          });

          test('pause-resume', () {
            var mock = new MockClient();
            registerQueryMock(mock, 70, 50);

            var api = new PubSub(mock, PROJECT);
            var count = 0;
            var subscription;
            subscription = api.listTopics().listen(
                expectAsync(((_) {
                  subscription..pause()..resume()..pause();
                  if ((count % 2) == 0) {
                    subscription.resume();
                  } else {
                    scheduleMicrotask(() => subscription.resume());
                  }
                  return null;
                }), count: 70),
                onDone: expectAsync(() => null))
                    ..pause();
            scheduleMicrotask(() => subscription.resume());
          });

          test('immediate-cancel', () {
            var mock = new MockClient();
            registerQueryMock(mock, 70, 50, 1);

            var api = new PubSub(mock, PROJECT);
            api.listTopics().listen(
                (_) => throw 'Unexpected',
                onDone: () => throw 'Unexpected')
                    ..cancel();
          });

          test('cancel', () {
            var mock = new MockClient();
            // There will be two calls to the mock as the cancel happen after
            // processing the first result which will trigger a second request.
            registerQueryMock(mock, 170, 50, 2);

            var api = new PubSub(mock, PROJECT);
            var subscription;
            subscription = api.listTopics().listen(
                expectAsync((_) => subscription.cancel()),
                onDone: () => throw 'Unexpected');
          });

          test('error', () {
            runTest(bool withPause) {
              // Test error on first GET request.
              var mock = new MockClient();
              mock.register('GET', 'topics', expectAsync((request) {
                return mock.respondError(500);
              }));
              var api = new PubSub(mock, PROJECT);
              var subscription;
              subscription = api.listTopics().listen(
                  (_) => throw 'Unexpected',
                  onDone: expectAsync(() => null),
                  onError: expectAsync(
                      (e) => e is pubsub.DetailedApiRequestError));
              if (withPause) {
                subscription.pause();
                scheduleMicrotask(() => subscription.resume());
              }
            }

            runTest(false);
            runTest(true);
          });

          test('error-2', () {
            // Test error on second GET request.
            void runTest(bool withPause) {
              var mock = new MockClient();
              registerQueryMock(mock, 51, 50, 1);

              var api = new PubSub(mock, PROJECT);

              int count = 0;
              var subscription;
              subscription = api.listTopics().listen(
                  expectAsync(((_) {
                    count++;
                    if (count == 50) {
                      if (withPause) {
                        subscription.pause();
                        scheduleMicrotask(() => subscription.resume());
                      }
                      mock.clear();
                      mock.register('GET', 'topics', expectAsync((request) {
                        return mock.respondError(500);
                      }));
                    }
                    return null;
                  }), count: 50),
                  onDone: expectAsync(() => null),
                  onError: expectAsync(
                      (e) => e is pubsub.DetailedApiRequestError));
            }

            runTest(false);
            runTest(true);
          });
        });

        group('page', () {
          test('empty', () {
            var mock = new MockClient();
            registerQueryMock(mock, 0, 50);

            var api = new PubSub(mock, PROJECT);
            return api.pageTopics().then(expectAsync((page) {
              expect(page.items.length, 0);
              expect(page.isLast, isTrue);
              expect(page.next(), completion(isNull));

              mock.clear();
              registerQueryMock(mock, 0, 20);
              return api.pageTopics(pageSize: 20).then(expectAsync((page) {
                expect(page.items.length, 0);
                expect(page.isLast, isTrue);
                expect(page.next(), completion(isNull));
              }));
            }));
          });

          test('single', () {
            var mock = new MockClient();
            registerQueryMock(mock, 10, 50);

            var api = new PubSub(mock, PROJECT);
            return api.pageTopics().then(expectAsync((page) {
              expect(page.items.length, 10);
              expect(page.isLast, isTrue);
              expect(page.next(), completion(isNull));

              mock.clear();
              registerQueryMock(mock, 20, 20);
              return api.pageTopics(pageSize: 20).then(expectAsync((page) {
                expect(page.items.length, 20);
                expect(page.isLast, isTrue);
                expect(page.next(), completion(isNull));
              }));
            }));
          });

          test('multiple', () {
            runTest(n, pageSize) {
              var totalPages = (n + pageSize - 1) ~/ pageSize;
              var pageCount = 0;

              var completer = new Completer();
              var mock = new MockClient();
              registerQueryMock(mock, n, pageSize);

              handlePage(page) {
                pageCount++;
                expect(page.isLast, pageCount == totalPages);
                expect(page.items.length,
                       page.isLast ? n - (totalPages - 1) * pageSize
                                   : pageSize );
                page.next().then(expectAsync((page) {
                  if (page != null) {
                    handlePage(page);
                  } else {
                    expect(pageCount, totalPages);
                    completer.complete();
                  }
                }));
              }

              var api = new PubSub(mock, PROJECT);
              api.pageTopics(pageSize: pageSize).then(expectAsync(handlePage));

              return completer.future;
            }

            return runTest(70, 50)
                .then((_) => runTest(99, 1))
                .then((_) => runTest(99, 50))
                .then((_) => runTest(99, 98))
                .then((_) => runTest(99, 99))
                .then((_) => runTest(99, 100))
                .then((_) => runTest(100, 1))
                .then((_) => runTest(100, 50))
                .then((_) => runTest(100, 100))
                .then((_) => runTest(101, 50));
          });
        });
      });
    });

    group('subscription', () {
      var name = 'test-subscription';
      var absoluteName = '/subscriptions/$PROJECT/test-subscription';
      var topicName = 'test-topic';
      var absoluteTopicName = '/topics/$PROJECT/test-topic';

      test('create', () {
        var mock = new MockClient();
        mock.register('POST', 'subscriptions', expectAsync((request) {
          var requestSubscription =
              new pubsub.Subscription.fromJson(JSON.decode(request.body));
          expect(requestSubscription.name, absoluteName);
          return mock.respond(new pubsub.Subscription()..name = absoluteName);
        }, count: 2));

        var api = new PubSub(mock, PROJECT);
        return api.createSubscription(name, topicName)
            .then(expectAsync((subscription) {
              expect(subscription.name, name);
              expect(subscription.absoluteName, absoluteName);
              return api.createSubscription(absoluteName, absoluteTopicName)
                  .then(expectAsync((subscription) {
                    expect(subscription.name, name);
                    expect(subscription.project, PROJECT);
                    expect(subscription.absoluteName, absoluteName);
                  }));
            }));
      });

      test('create-error', () {
        var mock = new MockClient();
        var api = new PubSub(mock, PROJECT);
        badSubscriptionNames.forEach((name) {
          expect(() => api.createSubscription(name, 'test-topic'),
                 throwsArgumentError);
        });
        badTopicNames.forEach((name) {
          expect(() => api.createSubscription('test-subscription', name),
                 throwsArgumentError);
        });
      });

      test('delete', () {
        var mock = new MockClient();
        mock.register(
            'DELETE',
            new RegExp(r'subscriptions/[a-z/-]*$'), expectAsync((request) {
          expect(request.url.path, '${ROOT_PATH}subscriptions/$absoluteName');
          expect(request.body.length, 0);
          return mock.respondEmpty();
        }, count: 2));

        var api = new PubSub(mock, PROJECT);
        return api.deleteSubscription(name).then(expectAsync((result) {
          expect(result, isNull);
          return api.deleteSubscription(absoluteName).then(expectAsync((topic) {
            expect(result, isNull);
          }));
        }));
      });

      test('delete-error', () {
        var mock = new MockClient();
        var api = new PubSub(mock, PROJECT);
        badSubscriptionNames.forEach((name) {
          expect(() => api.deleteSubscription(name), throwsArgumentError);
        });
        badTopicNames.forEach((name) {
          expect(() => api.deleteSubscription(name), throwsArgumentError);
        });
      });

      test('lookup', () {
        var mock = new MockClient();
        mock.register(
            'GET',
            new RegExp(r'subscriptions/[a-z/-]*$'), expectAsync((request) {
          expect(request.url.path, '${ROOT_PATH}subscriptions/$absoluteName');
          expect(request.body.length, 0);
          return mock.respond(new pubsub.Subscription()..name = absoluteName);
        }, count: 2));

        var api = new PubSub(mock, PROJECT);
        return api.lookupSubscription(name).then(expectAsync((subscription) {
          expect(subscription.name, name);
          expect(subscription.absoluteName, absoluteName);
          return api.lookupSubscription(absoluteName)
              .then(expectAsync((subscription) {
                expect(subscription.name, name);
                expect(subscription.project, PROJECT);
                expect(subscription.absoluteName, absoluteName);
              }));
        }));
      });

      test('lookup-error', () {
        var mock = new MockClient();
        var api = new PubSub(mock, PROJECT);
        badSubscriptionNames.forEach((name) {
          expect(() => api.lookupSubscription(name), throwsArgumentError);
        });
        badTopicNames.forEach((name) {
          expect(() => api.lookupSubscription(name), throwsArgumentError);
        });
      });

      group('query', () {
        var query = 'cloud.googleapis.com/project in (/projects/$PROJECT)';
        var topicQuery =
            'pubsub.googleapis.com/topic in (/topics/$PROJECT/topic)';
        var defaultPageSize = 50;

        addSubscriptions(
            pubsub.ListSubscriptionsResponse response, int first, int count) {
          response.subscription = [];
          for (int i = 0; i < count; i++) {
            response.subscription.add(
                new pubsub.Subscription()..name = 'subscription-${first + i}');
          }
        }


        // Mock that expect/generates [n] subscriptions in pages of page size
        // [pageSize].
        registerQueryMock(mock, n, pageSize, {String topic, int totalCalls}) {
          var totalPages = (n + pageSize - 1) ~/ pageSize;
          // No items still generate one request.
          if (totalPages == 0) totalPages = 1;
          // Can pass in total calls if this mock is overwritten before all
          // expected pages are done, e.g. when testing errors.
          if (totalCalls == null) {
            totalCalls = totalPages;
          }
          var pageCount = 0;
          mock.register('GET', 'subscriptions', expectAsync((request) {
            pageCount++;
            expect(request.url.queryParameters['query'],
                   topic == null ? query : topicQuery);
            expect(request.url.queryParameters['maxResults'], '$pageSize');
            expect(request.body.length, 0);
            if (pageCount > 1) {
              expect(request.url.queryParameters['pageToken'], 'next-page');
            }

            var response = new pubsub.ListSubscriptionsResponse();
            var first = (pageCount - 1) * pageSize + 1;
            if (pageCount < totalPages) {
              response.nextPageToken = 'next-page';
              addSubscriptions(response, first, pageSize);
            } else {
              addSubscriptions(
                  response, first, n - (totalPages - 1) * pageSize);
            }
            return mock.respond(response);
          }, count: totalCalls));
        }

        group('list', () {
          Future q(topic, count) {
            var mock = new MockClient();
            registerQueryMock(mock, count, 50, topic: topic);

            var api = new PubSub(mock, PROJECT);
            return api.listSubscriptions(topic).listen(
                expectAsync((_) => null, count: count)).asFuture();
          }

          test('simple', () {
            return q(null, 0)
                .then((_) => q('topic', 0))
                .then((_) => q(null, 1))
                .then((_) => q('topic', 1))
                .then((_) => q(null, 10))
                .then((_) => q('topic', 10))
                .then((_) => q(null, 49))
                .then((_) => q('topic', 49))
                .then((_) => q(null, 50))
                .then((_) => q('topic', 50))
                .then((_) => q(null, 51))
                .then((_) => q('topic', 51))
                .then((_) => q(null, 99))
                .then((_) => q('topic', 99))
                .then((_) => q(null, 100))
                .then((_) => q('topic', 100))
                .then((_) => q(null, 101))
                .then((_) => q('topic', 101))
                .then((_) => q(null, 170))
                .then((_) => q('topic', 170));
          });

          test('immediate-pause-resume', () {
            var mock = new MockClient();
            registerQueryMock(mock, 70, 50);

            var api = new PubSub(mock, PROJECT);
            api.listSubscriptions().listen(
                expectAsync(((_) => null), count: 70),
                onDone: expectAsync(() => null))
                    ..pause()
                    ..resume()
                    ..pause()
                    ..resume();
          });

          test('pause-resume', () {
            var mock = new MockClient();
            registerQueryMock(mock, 70, 50);

            var api = new PubSub(mock, PROJECT);
            var count = 0;
            var subscription;
            subscription = api.listSubscriptions().listen(
                expectAsync(((_) {
                  subscription..pause()..resume()..pause();
                  if ((count % 2) == 0) {
                    subscription.resume();
                  } else {
                    scheduleMicrotask(() => subscription.resume());
                  }
                  return null;
                }), count: 70),
                onDone: expectAsync(() => null))
                    ..pause();
            scheduleMicrotask(() => subscription.resume());
          });

          test('immediate-cancel', () {
            var mock = new MockClient();
            registerQueryMock(mock, 70, 50, totalCalls: 1);

            var api = new PubSub(mock, PROJECT);
            api.listSubscriptions().listen(
                (_) => throw 'Unexpected',
                onDone: () => throw 'Unexpected')
                    ..cancel();
          });

          test('cancel', () {
            var mock = new MockClient();
            // There will be two calls to the mock as the cancel happen after
            // processing the first result which will trigger a second request.
            registerQueryMock(mock, 170, 50, totalCalls: 2);

            var api = new PubSub(mock, PROJECT);
            var subscription;
            subscription = api.listSubscriptions().listen(
                expectAsync((_) => subscription.cancel()),
                onDone: () => throw 'Unexpected');
          });

          test('error', () {
            runTest(bool withPause) {
              // Test error on first GET request.
              var mock = new MockClient();
              mock.register('GET', 'subscriptions', expectAsync((request) {
                return mock.respondError(500);
              }));
              var api = new PubSub(mock, PROJECT);
              var subscription;
              subscription = api.listSubscriptions().listen(
                  (_) => throw 'Unexpected',
                  onDone: expectAsync(() => null),
                  onError: expectAsync(
                      (e) => e is pubsub.DetailedApiRequestError));
              if (withPause) {
                subscription.pause();
                scheduleMicrotask(() => subscription.resume());
              }
            }

            runTest(false);
            runTest(true);
          });

          test('error-2', () {
            runTest(bool withPause) {
              // Test error on second GET request.
              var mock = new MockClient();
              registerQueryMock(mock, 51, 50, totalCalls: 1);

              var api = new PubSub(mock, PROJECT);

              int count = 0;
              var subscription;
              subscription = api.listSubscriptions().listen(
                  expectAsync(((_) {
                    count++;
                    if (count == 50) {
                      if (withPause) {
                        subscription.pause();
                        scheduleMicrotask(() => subscription.resume());
                      }
                      mock.clear();
                      mock.register(
                          'GET', 'subscriptions', expectAsync((request) {
                        return mock.respondError(500);
                      }));
                    }
                    return null;
                  }), count: 50),
                  onDone: expectAsync(() => null),
                  onError: expectAsync(
                      (e) => e is pubsub.DetailedApiRequestError));
            }

            runTest(false);
            runTest(true);
          });
        });

        group('page', () {
          emptyTest(String topic) {
            var mock = new MockClient();
            registerQueryMock(mock, 0, 50, topic: topic);

            var api = new PubSub(mock, PROJECT);
            return api.pageSubscriptions(topic: topic).then(expectAsync((page) {
              expect(page.items.length, 0);
              expect(page.isLast, isTrue);
              expect(page.next(), completion(isNull));

              mock.clear();
              registerQueryMock(mock, 0, 20, topic: topic);
              return api.pageSubscriptions(topic: topic, pageSize: 20)
                  .then(expectAsync((page) {
                    expect(page.items.length, 0);
                    expect(page.isLast, isTrue);
                    expect(page.next(), completion(isNull));
                  }));
            }));
          }

          test('empty', () {
            emptyTest(null);
            emptyTest('topic');
          });

          singleTest(String topic) {
            var mock = new MockClient();
            registerQueryMock(mock, 10, 50, topic: topic);

            var api = new PubSub(mock, PROJECT);
            return api.pageSubscriptions(topic: topic).then(expectAsync((page) {
              expect(page.items.length, 10);
              expect(page.isLast, isTrue);
              expect(page.next(), completion(isNull));

              mock.clear();
              registerQueryMock(mock, 20, 20, topic: topic);
              return api.pageSubscriptions(topic: topic, pageSize: 20)
                  .then(expectAsync((page) {
                    expect(page.items.length, 20);
                    expect(page.isLast, isTrue);
                    expect(page.next(), completion(isNull));
                  }));
            }));
          }

          test('single', () {
            singleTest(null);
            singleTest('topic');
          });

          multipleTest(n, pageSize, topic) {
            var totalPages = (n + pageSize - 1) ~/ pageSize;
            var pageCount = 0;

            var completer = new Completer();
            var mock = new MockClient();
            registerQueryMock(mock, n, pageSize, topic: topic);

            handlingPage(page) {
              pageCount++;
              expect(page.isLast, pageCount == totalPages);
              expect(page.items.length,
                     page.isLast ? n - (totalPages - 1) * pageSize
                                 : pageSize );
              page.next().then((page) {
                if (page != null) {
                  handlingPage(page);
                } else {
                  expect(pageCount, totalPages);
                  completer.complete();
                }
              });
            }

            var api = new PubSub(mock, PROJECT);
            api.pageSubscriptions(topic: topic, pageSize: pageSize)
                .then(handlingPage);

            return completer.future;
          }

          test('multiple', () {
            return multipleTest(70, 50, null)
                .then((_) => multipleTest(99, 1, null))
                .then((_) => multipleTest(99, 50, null))
                .then((_) => multipleTest(99, 98, null))
                .then((_) => multipleTest(99, 99, null))
                .then((_) => multipleTest(99, 100, null))
                .then((_) => multipleTest(100, 1, null))
                .then((_) => multipleTest(100, 50, null))
                .then((_) => multipleTest(100, 100, null))
                .then((_) => multipleTest(101, 50, null))
                .then((_) => multipleTest(70, 50, 'topic'))
                .then((_) => multipleTest(99, 1, 'topic'))
                .then((_) => multipleTest(99, 50, 'topic'))
                .then((_) => multipleTest(99, 98, 'topic'))
                .then((_) => multipleTest(99, 99, 'topic'))
                .then((_) => multipleTest(99, 100, 'topic'))
                .then((_) => multipleTest(100, 1, 'topic'))
                .then((_) => multipleTest(100, 50, 'topic'))
                .then((_) => multipleTest(100, 100, 'topic'))
                .then((_) => multipleTest(101, 50, 'topic'));
          });
        });
      });
    });
  });

  group('topic', () {
    var name = 'test-topic';
    var absoluteName = '/topics/$PROJECT/test-topic';

    test('delete', () {
      var mock = new MockClient();
      mock.register(
          'GET', new RegExp(r'topics/[a-z/-]*$'), expectAsync((request) {
        expect(request.url.path, '${ROOT_PATH}topics/$absoluteName');
        expect(request.body.length, 0);
        return mock.respond(new pubsub.Topic()..name = absoluteName);
      }));

      var api = new PubSub(mock, PROJECT);
      return api.lookupTopic(name).then(expectAsync((topic) {
        expect(topic.name, name);
        expect(topic.absoluteName, absoluteName);

        mock.register(
            'DELETE', new RegExp(r'topics/[a-z/-]*$'), expectAsync((request) {
          expect(request.url.path, '${ROOT_PATH}topics/$absoluteName');
          expect(request.body.length, 0);
          return mock.respondEmpty();
        }));

        return topic.delete().then(expectAsync((result) {
          expect(result, isNull);
        }));
      }));
    });
  });

  group('subscription', () {
    var name = 'test-subscription';
    var absoluteName = '/subscriptions/$PROJECT/test-subscription';
    var topicName = 'test-topic';
    var absoluteTopicName = '/topics/$PROJECT/test-topic';

    test('delete', () {
      var mock = new MockClient();
      mock.register(
          'GET', new RegExp(r'subscriptions/[a-z/-]*$'), expectAsync((request) {
        expect(request.url.path, '${ROOT_PATH}subscriptions/$absoluteName');
        expect(request.body.length, 0);
        return mock.respond(new pubsub.Topic()..name = absoluteName);
      }));

      var api = new PubSub(mock, PROJECT);
      return api.lookupSubscription(name).then(expectAsync((subscription) {
        expect(subscription.name, name);
        expect(subscription.absoluteName, absoluteName);

        mock.register(
            'DELETE',
            new RegExp(r'subscriptions/[a-z/-]*$'), expectAsync((request) {
          expect(request.url.path, '${ROOT_PATH}subscriptions/$absoluteName');
          expect(request.body.length, 0);
          return mock.respondEmpty();
        }));

        return subscription.delete().then(expectAsync((result) {
          expect(result, isNull);
        }));
      }));
    });
  });

  group('push', () {
    var requestBody =
        '{"message":{"data":"SGVsbG8sIHdvcmxkIDMwIG9mIDUwIQ==",'
        '"labels":[{"key":"messageNo","numValue":30},'
                  '{"key":"test","strValue":"hello"}]},'
        '"subscription":"sgjesse-managed-vm/test-push-subscription"}';
    var event = new PushEvent.fromJson(requestBody);
    expect(event.message.asString, "Hello, world 30 of 50!");
    expect(event.message.labels['messageNo'], 30);
    expect(event.message.labels['test'], 'hello');
  });
}
