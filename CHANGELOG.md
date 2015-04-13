## 0.2.0+1

* Fix broken import of package:googleapis/common/common.dart.

## 0.2.0

* Add support for Cloud Pub/Sub.
* Require Dart version 1.9.

## 0.1.4+2

* Enforce fully populated entity keys in a number of places.

## 0.1.4+1

* Deduce the query partition automatically from query ancestor key.

## 0.1.4

* Added optional `defaultPartition` parameter to the constructor of
  `DatastoreDB`.

## 0.1.3+2

* Widened googleapis/googleapis_beta constraints in pubspec.yaml.

## 0.1.3+1

* Change the service scope keys keys to non-private symbols.

## 0.1.3

* Widen package:googleapis dependency constraint in pubspec.yaml.
* Bugfix in `package:appengine/db.dart`: Correctly handle ListProperties
of length 1.

## 0.1.2

* Introduced `package:gcloud/service_scope.dart` library.
* Added global getters for getting gcloud services from the current service
scope.
* Added an `package:gcloud/http.dart` library using service scopes.

## 0.1.1

* Increased version constraint on googleapis{,_auth,_beta}.

* Removed unused imports.

## 0.1.0

* First release.
