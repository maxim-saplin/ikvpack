import 'dart:html';
import 'dart:indexed_db';
import 'dart:async';
import 'dart:typed_data';

import 'bulkinsert_js.dart';
import '../ikvpack_core.dart';

final _connections = <String, Database>{}; // Path, DB

class Storage implements StorageBase {
  Storage(this.path);
  final String path;

  bool _disposed = false;

  @override
  void close() {
    _closeDbAt(path);
  }

  @override
  void dispose() {
    close();
    _disposed = true;
  }

  @override
  Future<Stats> getStats() {
    throw UnimplementedError();
  }

  @override
  bool get noOutOfOrderFlag => false;

  @override
  bool get noUpperCaseFlag => false;

  late Database _db;

  @override
  Future<List<String>> readSortedKeys() async {
    if (_disposed) throw 'Storage object was disposed, cant use it';
    _db = await _getDb(path);
    if (!_connections.containsKey(path)) {
      _connections[path] = _db;
    }
    var store = await _getKeyStoreInDb(_db);
    var request = store.getAll(null);
    var completer = Completer<List<String>>();
    request.onSuccess.listen((_) {
      var s = request.result as List<dynamic>;
      var k = s.cast<String>();
      completer.complete(k);
    });
    request.onError.listen((_) {
      completer.completeError(request.error!);
    });
    return completer.future;
  }

  @override
  void reopenFile() {}

  @override
  int get sizeBytes => -1;

  @override
  bool get useIndexToGetValue => true;

  @override
  Future<Uint8List> value(String key) async {
    throw UnimplementedError();
  }

  @override
  Future<Uint8List> valueAt(int index) async {
    var store = await _getValueStoreInDb(_db);
    var bytes = await store.getObject(index) as ByteBuffer;
    var list = bytes.asUint8List();
    return list;
  }
}

const String _storeKeys = 'keys';
const String _storeValues = 'values';

Future<Database> _getDb(String path, [bool clear = false]) async {
  if (clear) {
    _closeDbAt(path);
    await window.indexedDB!.deleteDatabase(path);
  }

  var db = await window.indexedDB!.open(path, version: 1, onUpgradeNeeded: (e) {
    var db = e.target.result as Database;
    if (!db.objectStoreNames!.contains(_storeKeys)) {
      db.createObjectStore(_storeKeys);
    }
    if (!db.objectStoreNames!.contains(_storeValues)) {
      db.createObjectStore(_storeValues);
    }
  });

  return db;
}

void _closeDbAt(String path) {
  if (_connections.containsKey(path)) {
    _connections[path]?.close();
    _connections.remove(path);
  }
}

Future<ObjectStore> _getKeyStoreInDb(Database db) async {
  return db.transaction(_storeKeys, 'readonly').objectStore(_storeKeys);
}

Future<ObjectStore> _getValueStoreInDb(Database db) async {
  return db.transaction(_storeValues, 'readonly').objectStore(_storeValues);
}

Future<void> saveToPath(String path, List<String> keys, List<Uint8List> values,
    [Function(int progressPercent)? updateProgress]) async {
  var db = await _getDb(path, true);
  var canceled = false;

  print('Inserting keys to IndexedDB ${path}');
  try {
    if (updateProgress == null || keys.length < 100) {
      await insert(db, keys, values);
    } else {
      var split = 20;
      var chunkSize = (keys.length / split).round();
      for (var i = 1; i < split; i++) {
        await insert(db, keys, values, chunkSize * (i - 1), chunkSize * i - 1);
        if (updateProgress(i * (100 / split).round()) == true) {
          print('Cancel');
          // if true - cancel
          canceled = true;
          break;
        }
      }
      if (!canceled) {
        await insert(
            db, keys, values, chunkSize * (split - 1), keys.length - 1);
        updateProgress(100);
      }
    }
  } finally {
    db.close();
    if (canceled) {
      deleteFromPath(path);
    }
  }
  print('Keys inserted to IndexedDB');
}

void deleteFromPath(String path) {
  _closeDbAt(path);
  window.indexedDB!.deleteDatabase(path);
}

Future<IkvInfo> storageGetInfo(String path) async {
  var db = await _getDb(path);
  var keys = await _getKeyStoreInDb(db);
  var count = await keys.count();
  return IkvInfo(-1, count, -1, -1);
}
