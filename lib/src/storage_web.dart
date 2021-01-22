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

  late ObjectStore _store;

  @override
  Future<List<String>> readSortedKeys() async {
    _store = await _getStoreInDb(path);
    var request = _store.getAllKeys(null);
    var completer = Completer<List<String>>();
    request.onSuccess.listen((_) {
      var s = request.result as List<dynamic>;
      var ss = s.cast<String>();
      completer.complete(ss);
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
  bool get useIndexToGetValue => false;

  @override
  Future<Uint8List> value(String key) async {
    //_store.getObject(key));
    return Uint8List(0);
  }

  @override
  Future<Uint8List> valueAt(int index) {
    throw UnimplementedError();
  }
}

const String _storeName = 'ikv';

Future<Database> _getDb(String path) async {
  var db = await window.indexedDB!.open(path, version: 1, onUpgradeNeeded: (e) {
    var db = e.target.result as Database;
    if (!db.objectStoreNames!.contains(_storeName)) {
      db.createObjectStore(_storeName);
    }
  });

  return db;
}

Future<ObjectStore> _getStoreInDb(String path) async {
  var db = await _getDb(path);
  return db.transaction(_storeName, 'readonly').objectStore(_storeName);
}

Future<void> saveToPath(
    String path, List<String> keys, List<Uint8List> values) async {
  var db = await _getDb(path);

  print('Inserting keys to IndexedDB..');
  await insert(db, keys, values);
  print('Keys inserted to IndexedDB');
}

void deleteFromPath(String path) {
  window.indexedDB!.deleteDatabase(path);
}

Future<IkvInfo> storageGetInfo(String path) async {
  var completer = Completer<IkvInfo>();
  completer.complete(IkvInfo(1, 1));
  return completer.future;
}
