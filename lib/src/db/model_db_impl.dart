// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of gcloud.db;


class ModelDBImpl implements ModelDB {
  // Map of properties for a given [ModelDescription]
  final Map<ModelDescriptionImpl, Map<String, Property>> _modelProperties = {};

  // Arbitrary state a model description might want to have
  final Map<ModelDescriptionImpl, Object> _modelDescriptionStates = {};

  // Needed when getting data from datastore to instantiate model objects.
  final Map<String, ModelDescriptionImpl> _modelDescriptionByKind = {};
  final Map<ModelDescriptionImpl, mirrors.ClassMirror> _modelClasses = {};
  final Map<ModelDescriptionImpl, Type> _typeByModelDescription = {};

  // Needed when application gives us model objects.
  final Map<Type, ModelDescriptionImpl> _modelDescriptionByType = {};


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
  ModelDBImpl() {
    // WARNING: This is O(n) of the source code, which is very bad!
    // Would be nice to have: `currentMirrorSystem().subclassesOf(Model)`
    _initialize(mirrors.currentMirrorSystem().libraries.values);
  }

  /**
   * Initializes a new [ModelDB] only using the library [librarySymbol].
   *
   * See also the default [ModelDB] constructor.
   */
  ModelDBImpl.fromLibrary(Symbol librarySymbol) {
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
      var idProperty = _modelProperties[modelDescription]['id'];
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

  String kindName(Type type) {
    return _modelDescriptionByType[type]._kind;
  }

  String fieldNameToPropertyName(String kind, String fieldName) {
    return _modelDescriptionByKind[kind].fieldNameToPropertyName(fieldName);
  }

  Iterable<ModelDescriptionImpl> get modelDescriptions {
    return _modelDescriptionByType.values;
  }

  Map<String, Property> propertiesForModel(
      ModelDescriptionImpl modelDescription) {
    return _modelProperties[modelDescription];
  }

  ModelDescriptionImpl modelDescriptionForType(Type type) {
    return _modelDescriptionByType[type];
  }

  mirrors.ClassMirror modelClass(ModelDescriptionImpl md) {
    return _modelClasses[md];
  }

  modelDescriptionState(ModelDescriptionImpl modelDescription) {
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

    var modelDescImpl;
    if (modelDesc is ExpandoModelDescription) {
      modelDescImpl = new ExpandoModelDescriptionImpl(modelDesc.kind);
    } else {
      modelDescImpl = new ModelDescriptionImpl(modelDesc.kind);
    }

    // Map the [modelClass.runtimeType] to this [modelDesc] and vice versa.
    _modelDescriptionByType[modelClass.reflectedType] = modelDescImpl;
    _typeByModelDescription[modelDescImpl] = modelClass.reflectedType;
    // Map this [modelDesc] to the [modelClass] mirror for easy instantiation.
    _modelClasses[modelDescImpl] = modelClass;

    // TODO: Move this out to the model description classes.

    // Get all properties, validate that the 'id' property is valid.
    var properties = _propertiesFromModelDescription(modelDesc);
    var idProperty = properties[ModelDescriptionImpl.ID_FIELDNAME];
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
    _modelProperties[modelDescImpl] = properties;

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

class ModelDescriptionImpl {
  static String ID_FIELDNAME = 'id';

  HashMap<String, String> property2FieldName;
  HashMap<String, String> field2PropertyName;
  Set<String> indexedProperties;
  Set<String> unIndexedProperties;

  final String _kind;

  ModelDescriptionImpl(this._kind);

  initialize(ModelDBImpl db) {
    // Compute propertyName -> fieldName mapping.
    property2FieldName = new HashMap<String, String>();
    field2PropertyName = new HashMap<String, String>();

    db.propertiesForModel(this).forEach((String fieldName, Property prop) {
      // The default of a datastore property name is the fieldName.
      // It can be overridden with [Property.propertyName].
      String propertyName = prop.propertyName;
      if (propertyName == null) propertyName = fieldName;

      if (fieldName != ID_FIELDNAME) {
        property2FieldName[propertyName] = fieldName;
        field2PropertyName[fieldName] = propertyName;
      }
    });

    // Compute properties & unindexed properties
    indexedProperties = new Set<String>();
    unIndexedProperties = new Set<String>();

    db.propertiesForModel(this).forEach((String fieldName, Property prop) {
      if (fieldName != ID_FIELDNAME) {
        String propertyName = prop.propertyName;
        if (propertyName == null) propertyName = fieldName;

        if (prop.indexed) {
          indexedProperties.add(propertyName);
        } else {
          unIndexedProperties.add(propertyName);
        }
      }
    });
  }

  bool registerKind(ModelDBImpl db) => true;

  String kindName(ModelDBImpl db) => _kind;

  datastore.Entity encodeModel(ModelDBImpl db, Model model) {
    var key = db.toDatastoreKey(model.key);

    var properties = {};
    var mirror = mirrors.reflect(model);

    db.propertiesForModel(this).forEach((String fieldName, Property prop) {
      _encodeProperty(db, model, mirror, properties, fieldName, prop);
    });

    return new datastore.Entity(
        key, properties, unIndexedProperties: unIndexedProperties);
  }

  _encodeProperty(ModelDBImpl db, Model model, mirrors.InstanceMirror mirror,
                  Map properties, String fieldName, Property prop) {
    String propertyName = prop.propertyName;
    if (propertyName == null) propertyName = fieldName;

    if (fieldName != ID_FIELDNAME) {
      var value = mirror.getField(
          mirrors.MirrorSystem.getSymbol(fieldName)).reflectee;
      if (!prop.validate(db, value)) {
        throw new StateError('Property validation failed for '
            'property $fieldName while trying to serialize entity of kind '
            '${model.runtimeType}. ');
      }
      properties[propertyName] = prop.encodeValue(db, value);
    }
  }

  Model decodeEntity(ModelDBImpl db, Key key, datastore.Entity entity) {
    if (entity == null) return null;

    // NOTE: this assumes a default constructor for the model classes!
    var classMirror = db.modelClass(this);
    var mirror = classMirror.newInstance(const Symbol(''), []);

    // Set the id and the parent key
    mirror.reflectee.id = key.id;
    mirror.reflectee.parentKey = key.parent;

    db.propertiesForModel(this).forEach((String fieldName, Property prop) {
      _decodeProperty(db, entity, mirror, fieldName, prop);
    });
    return mirror.reflectee;
  }

  _decodeProperty(ModelDBImpl db, datastore.Entity entity,
                  mirrors.InstanceMirror mirror, String fieldName,
                  Property prop) {
    String propertyName = fieldNameToPropertyName(fieldName);

    if (fieldName != ID_FIELDNAME) {
      var rawValue = entity.properties[propertyName];
      var value = prop.decodePrimitiveValue(db, rawValue);

      if (!prop.validate(db, value)) {
        throw new StateError('Property validation failed while '
            'trying to deserialize entity of kind '
            '${entity.key.elements.last.kind} (property name: $prop)');
      }

      mirror.setField(mirrors.MirrorSystem.getSymbol(fieldName), value);
    }
  }

  String fieldNameToPropertyName(String fieldName) {
    return field2PropertyName[fieldName];
  }

  String propertyNameToFieldName(ModelDBImpl db, String propertySearchName) {
    return property2FieldName[propertySearchName];
  }

  Object encodeField(ModelDBImpl db, String fieldName, Object value) {
    Property property = db.propertiesForModel(this)[fieldName];
    if (property != null) return property.encodeValue(db, value);
    return null;
  }
}

// NOTE/TODO:
// Currently expanded properties are only
//   * decoded if there are no clashes in [usedNames]
//   * encoded if there are no clashes in [usedNames]
// We might want to throw an error if there are clashes, because otherwise
//   - we may end up removing properties after a read-write cycle
//   - we may end up dropping added properties in a write
// ([usedNames] := [realFieldNames] + [realPropertyNames])
class ExpandoModelDescriptionImpl extends ModelDescriptionImpl {
  Set<String> realFieldNames;
  Set<String> realPropertyNames;
  Set<String> usedNames;

  ExpandoModelDescriptionImpl(String kind) : super(kind);

  initialize(ModelDBImpl db) {
    super.initialize(db);

    realFieldNames = new Set<String>.from(field2PropertyName.keys);
    realPropertyNames = new Set<String>.from(property2FieldName.keys);
    usedNames = new Set()..addAll(realFieldNames)..addAll(realPropertyNames);
  }

  datastore.Entity encodeModel(ModelDBImpl db, ExpandoModel model) {
    var entity = super.encodeModel(db, model);
    var properties = entity.properties;
    model.additionalProperties.forEach((String key, Object value) {
      // NOTE: All expanded properties will be indexed.
      if (!usedNames.contains(key)) {
        properties[key] = value;
      }
    });
    return entity;
  }

  Model decodeEntity(ModelDBImpl db, Key key, datastore.Entity entity) {
    if (entity == null) return null;

    ExpandoModel model = super.decodeEntity(db, key, entity);
    var properties = entity.properties;
    properties.forEach((String key, Object value) {
      if (!usedNames.contains(key)) {
        model.additionalProperties[key] = value;
      }
    });
    return model;
  }

  String fieldNameToPropertyName(String fieldName) {
    String propertyName = super.fieldNameToPropertyName(fieldName);
    // If the ModelDescription doesn't know about [fieldName], it's an
    // expanded property, where propertyName == fieldName.
    if (propertyName == null) propertyName = fieldName;
    return propertyName;
  }

  String propertyNameToFieldName(ModelDBImpl db, String propertyName) {
    String fieldName = super.propertyNameToFieldName(db, propertyName);
    // If the ModelDescription doesn't know about [propertyName], it's an
    // expanded property, where propertyName == fieldName.
    if (fieldName == null) fieldName = propertyName;
    return fieldName;
  }

  Object encodeField(ModelDBImpl db, String fieldName, Object value) {
    Object primitiveValue = super.encodeField(db, fieldName, value);
    // If superclass can't encode field, we return value here (and assume
    // it's primitive)
    // NOTE: Implicit assumption:
    // If value != null then superclass will return != null.
    // TODO: Ensure [value] is primitive in this case.
    if (primitiveValue == null) primitiveValue = value;
    return primitiveValue;
  }
}
