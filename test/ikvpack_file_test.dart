@TestOn('vm')

import 'dart:io';
import 'package:ikvpack/ikvpack.dart';
import 'package:isolate_pool_2/isolate_pool_2.dart';
import 'package:test/test.dart';

import 'shared.dart';
//import 'testMap.dart';

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

    File('test/test_bytes.dart').writeAsString(sb.toString());
  }

  //saveTestMapBytes();

  tearDownAll(() {
    var tmpDir = Directory('tmp');
    if (tmpDir.existsSync()) tmpDir.deleteSync(recursive: true);
  });

  group('File tests, case-insensitive', () {
    // saveTestMapBytes();
    // var ikv = IkvPack.fromMap(testMap, true);
    // ikv.saveTo('test/testIkv.dat');
    setUpAll(() async {
      setIkv(await IkvPack.load('test/testIkv.dat', true));
    });

    runCaseInvariantTests();
    runCaseInsensitiveTests();

    test('Stats is fecthed', () async {
      var stats = await ikv.getStats();
      expect(stats.keysNumber, 1445);
    });

    test('Stats is fecthed as CSV', () async {
      var stats = await IkvPack.getStatsAsCsv([ikv, ikv]);
      expect(stats.split('\n').length, 3);
    });
  });

  group('File tests', () {
    // runStorageInvariantTests();
    runFileTests();
    runFileAndIsolateTests();
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

void runFileTests() {
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
      await ik['a'];
    } catch (_) {
      thrown = true;
    }
    expect(thrown, true);
  });

  test('Files size is properly returned', () async {
    expect((await IkvPack.load('test/testIkv.dat')).sizeBytes, 269567);
  });

  test('IkvInfo is properly returned', () async {
    var info = await IkvPack.getInfo('test/testIkv.dat');
    expect(info.sizeBytes, 269567);
    expect(info.count, 1445);
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

    var keys = await IkvPack.consolidatedKeysStartingWith(ikvs, 'зьнізіць');
// Shadow keys used in case-insensitive, original key in sensitive, that's why same values are returned
// [0]:"зьнізіць"
// [1]:"зьнізіць"
    expect(keys.length, 2);
    expect(keys[0], 'зьнізіць');

    keys = await IkvPack.consolidatedKeysStartingWith(ikvs, 'b', 10);
    expect(keys.length, 10);
  });

  test('Flags are properly read from file', () async {
    var ikv00 = await IkvPack.load('test/testIkv.dat');
    expect(ikv00.noOutOfOrderFlag, false);
    expect(ikv00.noUpperCaseFlag, false);
    var ikv11 = await IkvPack.load('test/testIkvFlags.dat');
    expect(ikv11.noOutOfOrderFlag, true);
    expect(ikv11.noUpperCaseFlag, true);
  });

  test('Flags are respetcted when creating shadow keys', () async {
    var ikv11 = await IkvPack.load('test/testIkvFlags.dat');
    expect(ikv11.noOutOfOrderFlag, true);
    expect(ikv11.noUpperCaseFlag, true);
    expect(ikv11.shadowKeysUsed, false);
  });
}

void runFileAndIsolateTests() {
  test('IkvPack can be loaded in isolate', () async {
    var ik = await IkvPack.loadInIsolate('test/testIkv.dat', true);
    var v = await ik['зараць'];
    expect(v, '<div>вспахать</div>');
  });

  test('IkvPack error while loading in isolate is properly handled', () async {
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
      await ik.saveTo('tmp/test_isol$i.dat');
    }

    var futures = <Future<IkvPack>>[];

    for (var i = 0; i < 20; i++) {
      futures.add(IkvPack.loadInIsolatePool(pool, 'tmp/test_isol$i.dat'));
    }

    var res = await Future.wait(futures);
    pool.stop();

    // Odd behavior on Windows, files created in this test remain locked with some process
    for (var i = 0; i < 20; i++) {
      expect(res[i].length, 3);
      expect(await res[i]['c'], 'ccc');
      res[i].dispose();
      IkvPack.delete('tmp/test_isol$i.dat');
    }
  }, timeout: Timeout(Duration(seconds: 10)), onPlatform: {
    'windows': [
      Skip(
          'Odd behavior on Windows, files created in this test remain locked with some process')
    ]
  });
}
