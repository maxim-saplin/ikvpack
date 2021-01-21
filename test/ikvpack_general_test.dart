import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/ikvpack.dart';
import 'package:test/test.dart';

import 'shared.dart';
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

    test('Empty Values are deleted', () {
      var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
      var ik = IkvPack.fromMap(m);
      expect(ik.length, 2);
      expect(ik['wew'], 'dsdsd');
      expect(ik['sdss'], 'd');
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

    test('Single item key baskets)', () {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);
      expect(ik.length, 3);
      expect(ik['a'], 'aaa');
      expect(ik['b'], 'bbb');
      expect(ik['c'], 'ccc');
    });
  });
  group('In-memory tests, case-insensitive', () {
    setUp(() {
      setIkv(IkvPack.fromMap(testMap, true));
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();

    test('fromMap() constructor property inits object', () {
      //expect(ikv.indexedKeys, true);
      expect(ikv.valuesInMemory, true);
    });
  });

  group('In-memory tests, case-sensitive', () {
    setUp(() {
      setIkv(IkvPack.fromMap(testMap, false));
    });

    runCaseInvariantTests();
  });
}
