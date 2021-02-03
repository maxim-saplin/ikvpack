import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/ikvpack_core.dart';
import 'package:test/test.dart';

import 'shared.dart';
import 'testBytes.dart';
import 'testMap.dart';

void main() async {
  group('Corner cases', () {
    test('Passing empty Map throws AssertionError', () {
      var m = <String, String>{};
      expect(() => IkvPack.fromMap(m), throwsA(isA<AssertionError>()));
    });

    test('Passing empty Key throws AssertionError', () {
      var m = <String, String>{'': 'sdsd'};
      expect(() => IkvPack.fromMap(m), throwsA(isA<AssertionError>()));
    });

    test('Passing empty Value throws AssertionError', () {
      var m = <String, String>{'ss': ''};
      expect(() => IkvPack.fromMap(m), throwsA(isA<AssertionError>()));
    });

    test('Empty Keys are deleted', () {
      var m = <String, String>{
        '': 'sdsd',
        'wew': 'dsdsd',
        '\n\r': 'sdsd',
        'sdss': 'd'
      };
      var ik = IkvPack.fromMap(m);
      expect(ik.length, 2);
      expect(ik.containsKey('wew'), true);
      expect(ik.containsKey('sdss'), true);
    });

    test('Empty Values are deleted', () async {
      var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
      var ik = IkvPack.fromMap(m);
      expect(ik.length, 2);
      expect(await ik['wew'], 'dsdsd');
      expect(await ik['sdss'], 'd');
    });

    test('Keys are properly sanitized', () {
      var m = <String, String>{
        '\n\r': 'sdsd',
        '\r': 'asdads',
        '\n': 'sfsd',
        'sas\ndfsd': 'sdfs',
        'x' * 256: 'adsa'
      };
      var ik = IkvPack.fromMap(m);
      expect(ik.length, 2);
      expect(ik.keys[0], 'sasdfsd');
      expect(ik.keys[1].length, 255);
    });

    test('Single item key baskets)', () async {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);
      expect(ik.length, 3);
      expect(await ik['a'], 'aaa');
      expect(await ik['b'], 'bbb');
      expect(await ik['c'], 'ccc');
    });

    test('Non unique keys dont break anything', () async {
      // ignore: equal_keys_in_map
      var m = <String, String>{'a': 'aaa', 'a': 'aaa2', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);
      expect(ik.length, 3);
      var a = await ik['a'];
      expect(a == 'aaa' || a == 'aaa2', true);
      expect(await ik['b'], 'bbb');
      expect(await ik['c'], 'ccc');
    });
  });

  group('In-memory tests (Map), case-insensitive', () {
    setUpAll(() {
      setIkv(IkvPack.fromMap(testMap, true));
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();

    test('fromMap() constructor properly inits object', () {
      //expect(ikv.indexedKeys, true);
      expect(ikv.valuesInMemory, true);
    });

    test('IkvPack can return progress while being built from map', () async {
      var progressCalledTimes = 0;
      var maxProgress = 0;
      var ik = IkvPack.fromMap(testMap, true, (progress) {
        progressCalledTimes++;
        maxProgress = progress;
      });
      expect(ik.length, testMap.length);
      expect(progressCalledTimes > 2, true);
      expect(maxProgress, 100);
    });

    test('IkvPack can be built from map asynchronously and report progress',
        () async {
      var progressCalledTimes = 0;
      var maxProgress = 0;
      var ik = await IkvPack.buildFromMapAsync(testMap, true, (progress) async {
        progressCalledTimes++;
        maxProgress = progress;
        return 1;
      });
      expect(ik!.length, testMap.length);
      expect(progressCalledTimes > 2, true);
      expect(maxProgress, 100);
    });

    test('IkvPack asynchruos build from map can be canceled', () async {
      var progressCalledTimes = 0;
      var ik = await IkvPack.buildFromMapAsync(testMap, true, (progress) {
        progressCalledTimes++;
        if (progressCalledTimes > 2) return null;
      });
      expect(ik, null);
    });

    test('IkvPack can be built from map in isolate', () async {
      var progressCalledTimes = 0;
      var maxProgress = 0;
      var ik = await IkvPack.buildFromMapInIsolate(testMap, true, (progress) {
        progressCalledTimes++;
        maxProgress = progress;
      });
      expect(ik.length > 0, true);
      expect(progressCalledTimes > 2, true);
      expect(maxProgress, 100);
    }, testOn: 'vm');

    test('IkvPack keysStartingWith() returns unique shadow keys', () async {
      var m = <String, String>{
        'aaa': '_aaa',
        'Bbb': '_Bbb',
        'Aaa': '_Aaa',
        'sdss': 'd'
      };
      var ikv = IkvPack.fromMap(m);
      var matches = ikv.keysStartingWith('a', 100, true);
      expect(matches.length, 1);
      expect(matches[0], 'aaa');
      matches = ikv.keysStartingWith('aa', 100, true);
      expect(matches.length, 1);
      expect(matches[0], 'aaa');
    });
  });

  group('In-memory tests (Map), case-sensitive', () {
    setUpAll(() {
      setIkv(IkvPack.fromMap(testMap, false));
    });

    runCaseInvariantTests();
  });

  group('In-memory tests (ByteData), case-insensitive', () {
    setUpAll(() {
      setIkv(IkvPack.fromBytes(testBytes.buffer.asByteData(), true));
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();

    test('IkvPack can be built from bytes asynchronously and report progress',
        () async {
      var progressCalledTimes = 0;
      var maxProgress = 0;
      var ik = await IkvPack.buildFromBytesAsync(
          testBytes.buffer.asByteData(), true, (progress) async {
        progressCalledTimes++;
        maxProgress = progress;
        return 1;
      });
      expect(ik!.length, testMap.length);
      expect(progressCalledTimes > 2, true);
      expect(maxProgress, 100);
    });

    test('IkvPack asynchruos build from bytes can be canceled', () async {
      var progressCalledTimes = 0;
      var ik = await IkvPack.buildFromBytesAsync(
          testBytes.buffer.asByteData(), true, (progress) {
        progressCalledTimes++;
        if (progressCalledTimes > 2) return null;
      });
      expect(ik, null);
    });
  });
}
