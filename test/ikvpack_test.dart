import 'dart:io';
import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/ikvpack.dart';
import 'package:ikvpack/src/isolate_helpers.dart';
import 'package:test/test.dart';

import 'testMap.dart';

IkvPack? _ikv;

void runCommonTests(IkvPack ikv) {
  test('Can serach key by index', () {
    var k = ikv.keys[0];
    expect(k, '36');
    k = ikv.keys[1];
    expect(k, 'Aaron Burr');
  });

  test('Can get value by index', () {
    var v = ikv.valueAt(0);
    expect(
        v.startsWith(
            '<div><i>adjective</i></div><div>being six more than thirty'),
        true);
    v = ikv.valueAt(1);
    expect(
        v.startsWith(
            '<div><i>noun</i></div><div>United States politician who served'),
        true);
  });

  test('Can get value by key', () {
    var v = ikv['зараць'];
    expect(v, '<div>вспахать</div>');
  });

  test('Can get raw uncompressed value', () {
    var v = ikv.valueRawCompressed('зараць');
    expect(v.isNotEmpty, true);
    v = ikv.valueRawCompressedAt(2);
    expect(v.isNotEmpty, true);
  });

  test('Key keysStartingWith() finds the key', () {
    var keys = ikv.keysStartingWith('aerosol', 3);
    expect(keys[0], 'aerosol bomb');
  });

  test('Key in keysStartingWith() is trimmed', () {
    var keys = ikv.keysStartingWith(' Acer ', 1);
    expect(keys.length, 1);
  });

  test('Non existing keys return empty result', () {
    expect(ikv.containsKey('wewer'), false);
    expect(ikv['wewer'], '');
    expect(ikv.valueRawCompressed('wewer').isEmpty, true);
  });

  test('Consolidated keysStartingWith works on case sensitive keys',
      () => consolidatedTest(ikv, ikv));
}

dynamic consolidatedTest(IkvPack ikv1, IkvPack ikv2) {
  var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
  var ik = IkvPack.fromMap(m);
  var ikvs = [ikv1, ikv2, ik];

  var keys = IkvPack.consolidatedKeysStartingWith(ikvs, 'зьнізіць');

  expect(keys.length, 1);
  expect(keys[0], 'зьнізіць');

  keys = IkvPack.consolidatedKeysStartingWith(ikvs, 'b', 10);
  expect(keys.length, 10);
}

void runCaseInsensitiveTests(IkvPack ikv) {
  test('Out of order keys are fixed (ё isnt below я)', () {
    var k = ikv.keys[ikv.keys.length - 1];
    expect(k, 'яскравасьць');
  });

  test('Case-insensitive search by key works', () {
    var v = ikv['afrikaans'];
    expect(
        v.startsWith(
            '<div><b>I</b></div><div><i>noun</i></div><div>an official language of the'),
        true);
  });

  test('Keys are sorted', () {
    expect(ikv.keys[ikv.keys.length - 8] == 'эліпс', true);
    expect(ikv.keys[ikv.keys.length - 3] == 'юродзівасьць', true);
    expect(ikv.keys[ikv.keys.length - 1] == 'яскравасьць', true);
  });

  test('Key keysStartingWith() limits the result', () {
    var keys = ikv.keysStartingWith('an', 3);
    expect(keys.length, 3);
  });

  test('Key keysStartingWith() conducts case-insensitive search', () {
    var keys = ikv.keysStartingWith('ЗЬ');
    expect(keys.length, 6);
  });

  test(
      'Key keysStartingWith() search cases insesitive keys but returns original keys',
      () {
    var keys = ikv.keysStartingWith('неглижэ');
    expect(keys[0], 'негліжэ');
  });

  test('"и" and "i" subsitute wroks when looking up Belarusian words', () {
    var keys = ikv.keysStartingWith('ихтыёл');
    expect(keys.length, 1);
  });

  test('"и" and "i" subsitute wroks when getting value by original key', () {
    var keys = ikv.keysStartingWith('ихтыёл');
    expect(keys.length, 1);
    var val = ikv[keys[0]];
    expect(val, '<div>ихтиол</div>');
  });

  test('Consolidated keysStartingWith works on case-insensitive keys',
      () => consolidatedTest(ikv, ikv));
}

void runInMemoryRelatedTests(IkvPack ikv) {
  test('fromMap() constructor property inits object', () {
    //expect(ikv.indexedKeys, true);
    expect(ikv.valuesInMemory, true);
  });
}

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
    _ikv = IkvPack.fromMap(testMap, true);
    runCommonTests(_ikv!);
    runCaseInsensitiveTests(_ikv!);
    runInMemoryRelatedTests(_ikv!);
  });

  group('In-memory tests, case-sensitive', () {
    _ikv = IkvPack.fromMap(testMap, false);
    runCommonTests(_ikv!);
    runInMemoryRelatedTests(_ikv!);
  });

  group('File tests, case-insensitive', () {
    // _ikv = IkvPack.fromMap(testMap, true);
    // (_ikv!).saveTo('testIkv.dat');

    _ikv = IkvPack('test/testIkv.dat', true);

    runCommonTests(_ikv!);
    runCaseInsensitiveTests(_ikv!);

    test('IkvPack can return progress while being built from map', () async {
      var progressCalledTimes = 0;
      var maxProgress = 0;
      var ik = IkvPack.fromMap(testMap, true, (progress) {
        progressCalledTimes++;
        maxProgress = progress;
      });
      expect(ik.length > 0, true);
      expect(progressCalledTimes > 2, true);
      expect(maxProgress, 100);
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
    });
  });

  group('File tests, case-sensitive', () {
    _ikv = IkvPack('test/testIkv.dat', false);

    runCommonTests(_ikv!);
  });

  group('File tests', () {
    test('Disposed ikv cant be used anymore', () {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);

      expect(ik.length, 3);
      expect(ik['a'], 'aaa');
      expect(ik['b'], 'bbb');
      expect(ik['c'], 'ccc');

      ik.saveTo('tmp/test.dat');
      ik = IkvPack('tmp/test.dat');
      ik.dispose();

      expect(ik.length, 3);
      expect(() => ik['a'], throwsException);
    });

    test('IkvPack can be loaded in isolate', () async {
      var ik = await IkvPack.loadInIsolate('test/testIkv.dat', true);
      var v = ik['зараць'];
      expect(v, '<div>вспахать</div>');
    });

    test('IkvPacks can be loaded in isolate pool', () async {
      var pool = IsolatePool(4);
      await pool.start();
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};

      for (var i = 0; i < 20; i++) {
        var ik = IkvPack.fromMap(m);
        ik.saveTo('tmp/test${i}.dat');
      }

      var futures = <Future<IkvPack>>[];

      for (var i = 0; i < 20; i++) {
        futures.add(IkvPack.loadInIsolatePool(pool, 'tmp/test${i}.dat'));
      }

      var res = await Future.wait(futures);

      for (var i = 0; i < 20; i++) {
        expect(res[i].length, 3);
      }
    });

    test('Files size is properly returned', () {
      expect(IkvPack('test/testIkv.dat').sizeBytes, 263927);
    });

    test('File can be deleted', () {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);

      expect(ik.length, 3);

      ik.saveTo('tmp/test2.dat');

      expect(File('tmp/test2.dat').existsSync(), true);

      IkvPack.delete('tmp/test2.dat');
      expect(File('tmp/test2.dat').existsSync(), false);
    });

    test('Same data is read back', () {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);

      expect(ik.length, 3);
      expect(ik['a'], 'aaa');
      expect(ik['b'], 'bbb');
      expect(ik['c'], 'ccc');

      ik.saveTo('tmp/test.dat');
      ik = IkvPack('tmp/test.dat');

      expect(ik.length, 3);
      expect(ik['a'], 'aaa');
      expect(ik['b'], 'bbb');
      expect(ik['c'], 'ccc');
    });

    test(
        'Consolidated keysStartingWith works on mixed Ikvs ( both case- sensitive, insensitive)',
        () => consolidatedTest(IkvPack('test/testIkv.dat', true),
            IkvPack('test/testIkv.dat', false)));

    test('Flags are properly read from file', () {
      var ikv00 = IkvPack('test/testIkv.dat');
      expect(ikv00.noOutOfOrderFlag, false);
      expect(ikv00.noUpperCaseFlag, false);
      var ikv11 = IkvPack('test/testIkvFlags.dat');
      expect(ikv11.noOutOfOrderFlag, true);
      expect(ikv11.noUpperCaseFlag, true);
    });

    test('Flags are resetcted when creating shadow keys', () {
      var ikv11 = IkvPack('test/testIkvFlags.dat');
      expect(ikv11.noOutOfOrderFlag, true);
      expect(ikv11.noUpperCaseFlag, true);
      expect(ikv11.shadowKeysUsed, false);
    });
  });

  setUpAll(() {
    try {
      Directory('tmp').deleteSync(recursive: true);
    } catch (_) {}
    Directory('tmp').createSync();
  });

  tearDownAll(() {
    try {
      Directory('tmp').deleteSync(recursive: true);
    } catch (_) {}
  });
}
