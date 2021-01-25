@TestOn('vm')

import 'dart:io';
import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/ikvpack.dart';
import 'package:ikvpack/src/isolate_helpers.dart';
import 'package:test/test.dart';

import 'shared.dart';

void main() async {
  // Used to generate test data
  // ignore: unused_element
  void saveTestMapBytes() {
    var bytes = File('test/testIkv.dat').readAsBytesSync();
    var sb = StringBuffer();

    sb.writeln('import \'dart:typed_data\';\n');

    sb.write('final testBytes = Uint8List.fromList([');
    for (var i = 0; i < bytes.length; i++) {
      sb.write(bytes[i]);
      if (i != bytes.length - 1) {
        sb.write(', ');
      }
    }

    sb.write(']);');

    File('test/testBytes.dart').writeAsString(sb.toString());
  }

  group('File tests, case-insensitive', () {
    //saveTestMapBytes();
    // ikv = IkvPack.fromMap(testMap, true);
    // (ikv!).saveTo('testIkv.dat');
    setUpAll(() async {
      setIkv(await IkvPack.load('test/testIkv.dat', true));
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();
  });

  group('File tests', () {
    runStorageInvariantTests();
    test('Disposed ikv cant be used anymore', () async {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);

      expect(ik.length, 3);
      expect(await ik['a'], 'aaa');
      expect(await ik['b'], 'bbb');
      expect(await ik['c'], 'ccc');

      await ik.saveTo('tmp/test.dat');
      ik = await IkvPack.load('tmp/test.dat');
      ik.dispose();

      var thrown = false;
      try {
        var _ = await ik['a'];
      } catch (_) {
        thrown = true;
      }
      expect(thrown, true);
    });

    test('IkvPack can be loaded in isolate', () async {
      var ik = await IkvPack.loadInIsolate('test/testIkv.dat', true);
      var v = await ik['зараць'];
      expect(v, '<div>вспахать</div>');
    });

    test('IkvPack error while loading in isolate is properly handled',
        () async {
      var errorHandeled = false;

      try {
        await IkvPack.loadInIsolate('test2/testIkv.dat', true);
        expect('Never reached', 'Reached');
      } catch (_) {
        errorHandeled = true;
      }

      expect(errorHandeled, true);
    });

    test('IkvPacks can be loaded in isolate pool', () async {
      var pool = IsolatePool(4);
      await pool.start();
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};

      for (var i = 0; i < 20; i++) {
        var ik = IkvPack.fromMap(m);
        await ik.saveTo('tmp/test${i}.dat');
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

    test('Files size is properly returned', () async {
      expect((await IkvPack.load('test/testIkv.dat')).sizeBytes, 272695);
    });

    test('IkvInfo is properly returned', () async {
      var info = await IkvPack.getInfo('test/testIkv.dat');
      expect(info.sizeBytes, 272695);
      expect(info.length, 1436);
    });

    test('File can be deleted', () async {
      var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
      var ik = IkvPack.fromMap(m);

      expect(ik.length, 3);

      await ik.saveTo('tmp/test2.dat');

      expect(File('tmp/test2.dat').existsSync(), true);

      IkvPack.delete('tmp/test2.dat');
      expect(File('tmp/test2.dat').existsSync(), false);
    });

    test(
        'Consolidated keysStartingWith works on mixed Ikvs ( both case- sensitive, insensitive)',
        () async {
      var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
      var ik = IkvPack.fromMap(m);
      var ikvs = [
        await IkvPack.load('test/testIkv.dat', true),
        await IkvPack.load('test/testIkv.dat', false),
        ik
      ];

      var keys = IkvPack.consolidatedKeysStartingWith(ikvs, 'зьнізіць');
// Shadow keys used in case-insensitive, original key in sensitive, that's why same values are returned
// [0]:"зьнізіць"
// [1]:"зьнізіць"
      expect(keys.length, 2);
      expect(keys[0], 'зьнізіць');

      keys = IkvPack.consolidatedKeysStartingWith(ikvs, 'b', 10);
      expect(keys.length, 10);
    }, skip: false);

    test('Flags are properly read from file', () async {
      var ikv00 = await IkvPack.load('test/testIkv.dat');
      expect(ikv00.noOutOfOrderFlag, false);
      expect(ikv00.noUpperCaseFlag, false);
      var ikv11 = await IkvPack.load('test/testIkvFlags.dat');
      expect(ikv11.noOutOfOrderFlag, true);
      expect(ikv11.noUpperCaseFlag, true);
    });

    test('Flags are resetcted when creating shadow keys', () async {
      var ikv11 = await IkvPack.load('test/testIkvFlags.dat');
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
