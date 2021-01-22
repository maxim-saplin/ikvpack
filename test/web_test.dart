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
      setIkv(await IkvPack.load('test/testIkv.dat', true));
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
    test('IkvInfo is properly returned', () async {
      var info = await IkvPack.getInfo('test/testIkv.dat');
      expect(info.sizeBytes, -1);
      expect(info.length, 1436);
    });

    test('Same data is read back', () async {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);

      expect(ik.length, 3);
      expect(await ik['a'], 'aaa');
      expect(await ik['b'], 'bbb');
      expect(await ik['c'], 'ccc');

      await ik.saveTo('tmp/test.dat');
      ik = await IkvPack.load('tmp/test.dat');

      expect(ik.length, 3);
      expect(await ik['a'], 'aaa');
      expect(await ik['b'], 'bbb');
      expect(await ik['c'], 'ccc');
    });
  });
}
