// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of gcloud.db;


// TODO: We might move some of the complexity of this class to
// [ModelDescription]!

/**
 * Represents an in-memory database of all model classes and it's corresponding
 * [ModelDescriptions]s.
 */
class ModelDB {
  // Map of properties for a given [ModelDescription]
  final Map<ModelDescription, Map<String, Property>> _modelProperties = {};

  // Arbitrary state a model description might want to have
  final Map<ModelDescription, Object> _modelDescriptionStates = {};

  // Needed when getting data from datastore to instantiate model objects.
  final Map<String, ModelDescription> _modelDescriptionByKind = {};
  final Map<ModelDescription, mirrors.ClassMirror> _modelClasses = {};
  final Map<ModelDescription, Type> _typeByModelDescription = {};

  // Needed when application gives us model objects.
  final Map<Type, ModelDescription> _modelDescriptionByType = {};


  /**
   * Initializes a new [ModelDB] from all libraries.
   *
   * This will scan all libraries for [Model] classes and their
   * [ModelDescription] annotations. It will also scan all [Property] instances
   * on all [ModelDescription] objects.
   *
   * Once all libraries have been scanned it will call each [ModelDescription]s
   * 'initialize' method and stores the returned state object (this can be
   * queried later with [modelDescriptionState].
   *
   * Afterwards every [ModelDescription] will be asked whether it wants to
   * register a kind name and if so, that kind name will be associated with it.
   *
   * In case an error is encountered (e.g. two [ModelDescription] classes with
   * the same kind name) a [StateError] will be thrown.
   */
  ModelDB() {
    // WARNING: This is O(n) of the source code, which is very bad!
    // Would be nice to have: `currentMirrorSystem().subclassesOf(Model)`
    _initialize(mirrors.currentMirrorSystem().libraries.values);
  }

  /**
   * Initializes a new [ModelDB] only using the library [librarySymbol].
   *
   * See also the default [ModelDB] constructor.
   */
  ModelDB.fromLibrary(Symbol librarySymbol) {
    _initialize([mirrors.currentMirrorSystem().findLibrary(librarySymbol)]);
  }


  /**
   * Converts a [datastore.Key] to a [Key].
   */
  Key fromDatastoreKey(datastore.Key datastoreKey) {
    var namespace = new Partition(datastoreKey.partition.namespace);
    Key key = namespace.emptyKey;
    for (var element in datastoreKey.elements) {
      var type = _typeByModelDescription[_modelDescriptionByKind[element.kind]];
      assert (type != null);
      key = key.append(type, id: element.id);
    }
    return key;
  }

  /**
   * Converts a [Key] to a [datastore.Key].
   */
  datastore.Key toDatastoreKey(Key dbKey) {
    List<datastore.KeyElement> elements = [];
    var currentKey = dbKey;
    while (!currentKey.isEmpty) {
      var id = currentKey.id;

      var modelDescription = modelDescriptionForType(currentKey.type);
      var idProperty =
          propertiesForModel(modelDescription)[ModelDescription.ID_FIELDNAME];
      var kind = modelDescription.kindName(this);

      if (idProperty is IntProperty && (id != null && id is! int)) {
        throw new ArgumentError('Expected an integer id property but '
            'id was of type ${id.runtimeType}');
      }
      if (idProperty is StringProperty && (id != null && id is! String)) {
        throw new ArgumentError('Expected a string id property but '
            'id was of type ${id.runtimeType}');
      }

      elements.add(new datastore.KeyElement(kind, id));
      currentKey = currentKey.parent;
    }
    Partition partition = currentKey._parent;
    return new datastore.Key(
        elements.reversed.toList(),
        partition: new datastore.Partition(partition.namespace));
  }

  /**
   * Converts a [Model] instance to a [datastore.Entity].
   */
  datastore.Entity toDatastoreEntity(Model model) {
    try {
      var modelDescription = modelDescriptionForType(model.runtimeType);
      return modelDescription.encodeModel(this, model);
    } catch (error, stack) {
      throw
          new ArgumentError('Error while encoding entity ($error, $stack).');
    }
  }

  /**
   * Converts a [datastore.Entity] to a [Model] instance.
   */
  Model fromDatastoreEntity(datastore.Entity entity) {
    if (entity == null) return null;

    Key key = fromDatastoreKey(entity.key);
    var kind = entity.key.elements.last.kind;
    var modelDescription = _modelDescriptionByKind[kind];
    if (modelDescription == null) {
      throw new StateError('Trying to deserialize entity of kind '
          '$kind, but no Model class available for it.');
    }

    try {
      return modelDescription.decodeEntity(this, key, entity);
    } catch (error, stack) {
      throw new StateError('Error while decoding entity ($error, $stack).');
    }
  }


  Iterable<ModelDescription> get modelDescriptions {
    return _modelDescriptionByType.values;
  }

  Map<String, Property> propertiesForModel(
      ModelDescription modelDescription) {
    return _modelProperties[modelDescription];
  }

  ModelDescription modelDescriptionForType(Type type) {
    return _modelDescriptionByType[type];
  }

  mirrors.ClassMirror modelClass(ModelDescription md) {
    return _modelClasses[md];
  }

  modelDescriptionState(ModelDescription modelDescription) {
    return _modelDescriptionStates[modelDescription];
  }


  void _initialize(Iterable<mirrors.LibraryMirror> libraries) {
    libraries.forEach((mirrors.LibraryMirror lm) {
      lm.declarations.values
          .where((d) => d is mirrors.ClassMirror && d.hasReflectedType)
          .forEach((mirrors.ClassMirror declaration) {
        var modelDescription = _descriptionFromModelClass(declaration);
        if (modelDescription != null) {
          _newModelDescription(declaration, modelDescription);
        }
      });
    });

    // Ask every [ModelDescription] to compute whatever global state it wants
    // to have.
    for (var modelDescription in modelDescriptions) {
      _modelDescriptionStates[modelDescription] =
          modelDescription.initialize(this);
    }


    // Ask every [ModelDescription] whether we should register it with a given
    // kind name.
    for (var modelDescription in modelDescriptions) {
      if (modelDescription.registerKind(this)) {
        var kindName = modelDescription.kindName(this);
        if (_modelDescriptionByKind.containsKey(kindName)) {
          throw new StateError(
              'Cannot have two ModelDescriptions '
              'with the same kind ($kindName)');
        }
        _modelDescriptionByKind[kindName] = modelDescription;
      }
    }
  }

  void _newModelDescription(mirrors.ClassMirror modelClass,
                       ModelDescription modelDesc) {
    assert (!_modelDescriptionByType.containsKey(modelClass.reflectedType));

    // Map the [modelClass.runtimeType] to this [modelDesc] and vice versa.
    _modelDescriptionByType[modelClass.reflectedType] = modelDesc;
    _typeByModelDescription[modelDesc] = modelClass.reflectedType;
    // Map this [modelDesc] to the [modelClass] mirror for easy instantiation.
    _modelClasses[modelDesc] = modelClass;

    // TODO: Move this out to the model description classes.

    // Get all properties, validate that the 'id' property is valid.
    var properties = _propertiesFromModelDescription(modelDesc);
    var idProperty = properties[ModelDescription.ID_FIELDNAME];
    if (idProperty == null ||
        (idProperty is! IntProperty && idProperty is! StringProperty)) {
      throw new StateError(
          'You need to have an id property and it has to be either an '
          '[IntProperty] or a [StringProperty].');
    }
    if (idProperty.propertyName != null) {
      throw new StateError(
          'You can not have a new name for the id property.');
    }
    _modelProperties[modelDesc] = properties;

    // Ensure we have an empty constructor.
    bool defaultConstructorFound = false;
    for (var declaration in modelClass.declarations.values) {
      if (declaration is mirrors.MethodMirror) {
        if (declaration.isConstructor &&
            declaration.constructorName == const Symbol('') &&
            declaration.parameters.length == 0) {
          defaultConstructorFound = true;
          break;
        }
      }
    }
    if (!defaultConstructorFound) {
      throw new StateError(
          'Class ${modelClass.simpleName} does not have a default '
          'constructor.');
    }
  }

  // TODO: Move this out to the model description classes.
  Map<String, Property> _propertiesFromModelDescription(
      ModelDescription modelDescription) {
    var modelMirror = mirrors.reflect(modelDescription);
    var modelClassMirror = mirrors.reflectClass(modelDescription.runtimeType);

    var properties = new Map<String, Property>();
    var propertyNames = new Set<String>();

    // Loop over all classes in the inheritence path up to the Object class.
    while (modelClassMirror.superclass != null) {
      var memberMap = modelClassMirror.instanceMembers;
      // Loop over all declarations (which includes fields)
      modelClassMirror.declarations.forEach((Symbol s, _) {
        // Look if we do have a method for [s]
        if (memberMap.containsKey(s) && memberMap[s].isGetter) {
          // Get a String representation of the field and the value.
          var fieldName = mirrors.MirrorSystem.getName(s);
          var fieldValue = modelMirror.getField(s).reflectee;
          // If the field value is a Property instance we add it to the list
          // of properties.
          // Fields with '__' are reserved and will not be used.
          if (!fieldName.startsWith('__') &&
              fieldValue != null &&
              fieldValue is Property) {
            var propertyName = fieldValue.propertyName;
            if (propertyName == null) propertyName = fieldName;

            if (properties.containsKey(fieldName)) {
              throw new StateError(
                  'Cannot have two Property objects describing the same Model '
                  'property name in a ModelDescription class hierarchy.');
            }

            if (propertyNames.contains(propertyName)) {
              throw new StateError(
                  'Cannot have two Property objects mapping to the same '
                  'datastore property name ($propertyName).');
            }
            properties[fieldName] = fieldValue;
            propertyNames.add(propertyName);
          }
        }
      });
      modelClassMirror = modelClassMirror.superclass;
    }

    return properties;
  }

  ModelDescription _descriptionFromModelClass(mirrors.ClassMirror classMirror) {
    var result;
    for (mirrors.InstanceMirror instance in classMirror.metadata) {
      if (instance.reflectee.runtimeType == ModelMetadata) {
        if (result != null) {
          throw new StateError(
              'Cannot have more than one ModelMetadata() annotation '
              'on a Model class');
        }
        result = instance.getField(#description).reflectee;
      }
    }
    return result;
  }
}
