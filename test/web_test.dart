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

  group('Web/IndexedDB tests, in-memory case-insensitive', () {
    setUpAll(() {
      var ik = IkvPack.fromMap(testMap);
      setIkv(ik);
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();
  });

  // test('Same keys are read back', () async {
  //   var m = <String, String>{'wew': 'dsdsd', 'sdss': 'd'};
  //   var ik = IkvPack.fromMap(m);
  //   await ik.saveTo('test/testIkv.dat');
  //   ik = await IkvPack.load('test/testIkv.dat');
  //   expect(ik.keys[0], 'sdss');
  //   expect(ik.keys[1], 'wew');
  //   expect(ik.length, 2);
  // });

  // test('Same values are read back', () async {
  //   var m = <String, String>{'wew': 'dsdsd', 'sdss': 'd'};
  //   var ik = IkvPack.fromMap(m);
  //   await ik.saveTo('test/testIkv.dat');
  //   ik = await IkvPack.load('test/testIkv.dat');
  //   expect(await ik['wew'], 'dsdsd');
  //   expect(await ik['sdss'], 'd');
  //   expect(ik.length, 2);
  // });
}
