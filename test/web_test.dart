@TestOn('chrome')

import 'dart:html';

@TestOn('chrome')
import 'package:ikvpack/ikvpack.dart';
import 'package:test/test.dart';

import 'shared.dart';
import 'testMap.dart';

void main() async {
  group('Web/IndexedDB tests, db, case-insensitive', () {
    setUpAll(() async {
      var ik = IkvPack.fromMap(testMap);
      await ik.saveTo('test/testIkv.dat');
      ik = await IkvPack.load('test/testIkv.dat', true);
      setIkv(ik);
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();
  });

  group('Web/IndexedDB tests, in-memory, case-insensitive', () {
    setUpAll(() {
      var ik = IkvPack.fromMap(testMap);
      setIkv(ik);
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();
  });

  group('Web/IndexedDB tests', () {
    runStorageInvariantTests();

    test('IkvInfo is properly returned', () async {
      var info = await IkvPack.getInfo('test/testIkv.dat');
      expect(info.sizeBytes, -1);
      expect(info.length, 1436);
    });

    test('IndexedDB can be deleted', () async {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);

      expect(ik.length, 3);

      await ik.saveTo('tmp/test2.dat');

      var exists = false;

      var db = await window.indexedDB!.open('tmp/test2.dat');
      if (db.objectStoreNames!.contains('keys')) {
        exists = true;
      }

      expect(exists, true);
      db.close();
      IkvPack.delete('tmp/test2.dat');

      await Future.delayed(Duration(milliseconds: 100));

      exists = false;

      db = await window.indexedDB!.open('tmp/test2.dat');
      if (db.objectStoreNames!.contains('keys')) {
        exists = true;
      }

      expect(exists, false);
    });

    test('IkvPack can return progress while saving', () async {
      var progressCalledTimes = 0;
      var maxProgress = 0;
      var ik = IkvPack.fromMap(testMap);

      await ik.saveTo('tmp/test2.dat', (progress) {
        progressCalledTimes++;
        maxProgress = progress;
      });
      expect(progressCalledTimes > 2, true);
      expect(maxProgress, 100);
    });

    test('IkvPack can cancel saving', () async {
      var progressCalledTimes = 0;
      var maxProgress = 0;
      var ik = IkvPack.fromMap(testMap);

      await ik.saveTo('tmp/test2.dat', (progress) {
        progressCalledTimes++;
        maxProgress = progress;
        if (progress == 15) return true;
      });
      expect(progressCalledTimes > 2, true);
      expect(maxProgress, 15);

      var exists = false;

      var db = await window.indexedDB!.open('tmp/test2.dat');
      if (db.objectStoreNames!.contains('keys')) {
        exists = true;
      }

      expect(exists, false);
    });
  });
}
