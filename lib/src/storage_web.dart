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
  void closeFile() {}

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

  @override
  Future<List<String>> readSortedKeys() {
    throw UnimplementedError();
  }

  @override
  void reopenFile() {}

  @override
  int get sizeBytes => throw UnimplementedError();

  @override
  bool get useIndexToGetValue => throw UnimplementedError();

  @override
  Uint8List value(String key) {
    throw UnimplementedError();
  }

  @override
  Uint8List valueAt(int index) {
    throw UnimplementedError();
  }
}

Future<Database> getDb(String path) async {
  var db = await window.indexedDB!.open(path, version: 1, onUpgradeNeeded: (e) {
    var db = e.target.result as Database;
    if (!db.objectStoreNames!.contains('ikv')) {
      db.createObjectStore('ikv');
    }
  });

  return db;
}

Future<void> saveToPath(
    String path, List<String> keys, List<Uint8List> values) async {
  var db = await getDb(path);

  print('Inserting keys to IndexedDB..');
  await insert(db, keys, values);
  print('Keys inserted to IndexedDB');
}

void deleteFromPath(String path) {}

Future<IkvInfo> storageGetInfo(String path) async {
  var completer = Completer<IkvInfo>();
  completer.complete(IkvInfo(1, 1));
  return completer.future;
}
