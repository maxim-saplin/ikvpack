@TestOn('vm')

import 'package:ikvpack/ikvpack.dart';
import 'package:isolate_pool_2/isolate_pool_2.dart';
import 'package:test/test.dart';

import 'shared.dart';

void main() async {
  late IsolatePool pool;
  group('File tests', () {
    setUpAll(() async {
      pool = IsolatePool(4);
      await pool.start();
      var ikv = await IkvPackProxy.loadInIsolatePoolAndUseProxy(
          pool, 'test/testIkv.dat');
      setIkv(ikv);
    });

    runCaseInvariantTests(true);
    runCaseInsensitiveTests(true);
    test('Files size is properly returned', () async {
      expect(
          (await IkvPack.loadInIsolatePoolAndUseProxy(pool, 'test/testIkv.dat'))
              .sizeBytes,
          269567);
    });

    test(
        'Consolidated keysStartingWith works on mixed Ikvs ( both case- sensitive, insensitive)',
        () async {
      var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
      var ik = IkvPack.fromMap(m);
      var ikvs = [
        await IkvPackProxy.loadInIsolatePoolAndUseProxy(
            pool, 'test/testIkv.dat', true),
        await IkvPackProxy.loadInIsolatePoolAndUseProxy(
            pool, 'test/testIkv.dat', false),
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
      var ikv00 = await IkvPackProxy.loadInIsolatePoolAndUseProxy(
          pool, 'test/testIkv.dat');
      expect(ikv00.noOutOfOrderFlag, false);
      expect(ikv00.noUpperCaseFlag, false);
      var ikv11 = await IkvPackProxy.loadInIsolatePoolAndUseProxy(
          pool, 'test/testIkvFlags.dat');
      expect(ikv11.noOutOfOrderFlag, true);
      expect(ikv11.noUpperCaseFlag, true);
    });

    test('Flags are respetcted when creating shadow keys', () async {
      var ikv11 = await IkvPackProxy.loadInIsolatePoolAndUseProxy(
          pool, 'test/testIkvFlags.dat');
      expect(ikv11.noOutOfOrderFlag, true);
      expect(ikv11.noUpperCaseFlag, true);
      expect(ikv11.shadowKeysUsed, false);
    });
  });
}
