import 'dart:html';
import 'dart:indexed_db';
import 'dart:async';
import 'dart:typed_data';

import 'bulkinsert_js.dart';
import 'ikvpack.dart';

class Storage implements StorageBase {
  Storage(this.path);
  final String path;

  @override
  void close() {}

  @override
  void dispose() {}

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
    _db = await _getDb(path);
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

Future<Database> _getDb(String path) async {
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

Future<ObjectStore> _getKeyStoreInDb(Database db) async {
  return db.transaction(_storeKeys, 'readonly').objectStore(_storeKeys);
}

Future<ObjectStore> _getValueStoreInDb(Database db) async {
  return db.transaction(_storeValues, 'readonly').objectStore(_storeValues);
}

Future<void> saveToPath(
    String path, List<String> keys, List<Uint8List> values) async {
  var db = await _getDb(path);

  print('Inserting keys to IndexedDB..');
  await insert(db, keys, values);
  db.close();
  print('Keys inserted to IndexedDB');
}

void deleteFromPath(String path) {
  window.indexedDB!.deleteDatabase(path);
}

Future<IkvInfo> storageGetInfo(String path) async {
  var db = await _getDb(path);
  var keys = await _getKeyStoreInDb(db);
  var count = await keys.count();
  return IkvInfo(-1, count);
}
