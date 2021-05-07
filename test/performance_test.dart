@TestOn('vm')

import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'testMap.dart';
import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack_100/ikvpack.dart' as ikv100;
import 'package:ikvpack_200/ikvpack.dart' as ikv200;

const ruFile100 = 'test/performance_data/100/RU_EN Multitran vol.2.dikt';
const enFile100 = 'test/performance_data/100/EN_RU Multitran vol.2.dikt';
const ruFile200 = 'test/performance_data/200/RU_EN Multitran vol.2.dikt';
const enFile200 = 'test/performance_data/200/EN_RU Multitran vol.2.dikt';
const ruFileCurr = 'test/performance_data/RU_EN Multitran vol.2.dikt';
const enFileCurr = 'test/performance_data/EN_RU Multitran vol.2.dikt';

void main() async {
  const skippLongTests = true;

  group('Real file tests', () {
    Future<int> loadCurr(String path, bool keysCaseInsensitive,
        [IsolatePool? pool]) async {
      var ikv = pool == null
          ? await IkvPack.load(path, keysCaseInsensitive)
          : await IkvPack.loadInIsolatePool(pool, path);
      return ikv.length;
    }

    Future<int> loadIkv100(String path, bool keysCaseInsensitive,
        [ikv100.IsolatePool? pool]) async {
      var ikv = pool == null
          ? ikv100.IkvPack(path, keysCaseInsensitive)
          : await ikv100.IkvPack.loadInIsolatePool(pool, path);

      return ikv.length;
    }

    Future<int> loadIkv200(String path, bool keysCaseInsensitive,
        [ikv200.IsolatePool? pool]) async {
      var ikv = pool == null
          ? await ikv200.IkvPack.load(path, keysCaseInsensitive)
          : await ikv200.IkvPack.loadInIsolatePool(pool, path);
      return ikv.length;
    }

    test('Case-Insensitive, Current version not slower to load', () async {
      // var ikv = IkvPack(ruFile, false);
      // var stats = ikv.getStats();
      // ikv = IkvPack(enFile, false);
      // stats = ikv.getStats();

      // 02.02.2021 curr and Ikv200 are exactly the samem though first test is always faster, swapping lines confirms.
      // I susspect there's more grabage collection happenig in latter tests which creates the slowdown
      // that's why there's separate warm app pass with no resulats gathered

      print('Warming up');

      await _benchmark(() => loadCurr(ruFileCurr, true), 2, 1);
      await _benchmark(() => loadIkv200(ruFile200, true), 2, 1);
      await _benchmark(() => loadIkv100(ruFile100, true), 2, 1);

      print('Testing...');

      var currentRuInsense =
          await _benchmark(() => loadCurr(ruFileCurr, true), 6, 1);
      var ikv200RuInsense =
          await _benchmark(() => loadIkv200(ruFile200, true), 6, 1);
      var ikv100RuInsense =
          await _benchmark(() => loadIkv100(ruFile100, true), 6, 1);

      var currentEnInsense =
          await _benchmark(() => loadCurr(enFileCurr, true), 6, 1);
      var ikv200EnInsense =
          await _benchmark(() => loadIkv200(enFile200, true), 6, 1);
      var ikv100EnInsense =
          await _benchmark(() => loadIkv100(enFile100, true), 6, 1);

      currentRuInsense.prnt('CURR RU, CASE-INSE', true);
      ikv200RuInsense.prnt('I200 RU, CASE-INSE', true);
      ikv100RuInsense.prnt('I100 RU, CASE-INSE', true);

      currentEnInsense.prnt('CURR EN, CASE-INSE', true);
      ikv200EnInsense.prnt('I200 EN, CASE-INSE', true);
      ikv100EnInsense.prnt('I100 EN, CASE-INSE', true);

      expect(
          (currentRuInsense.avgMicro - ikv100RuInsense.avgMicro) *
                  100 /
                  ikv100RuInsense.avgMicro <
              10,
          true);
      expect(
          (currentEnInsense.avgMicro - ikv100EnInsense.avgMicro) *
                  100 /
                  ikv100EnInsense.avgMicro <
              10,
          true);

      expect(
          (currentRuInsense.avgMicro - ikv200RuInsense.avgMicro) *
                  100 /
                  ikv200RuInsense.avgMicro <
              10,
          true);
      expect(
          (currentEnInsense.avgMicro - ikv200EnInsense.avgMicro) *
                  100 /
                  ikv200EnInsense.avgMicro <
              10,
          true);
    }, skip: skippLongTests);

    test('Case-Sensitive, Current version not slower to load', () async {
      print('Warming up');

      await _benchmark(() => loadCurr(ruFileCurr, false), 2, 1);
      await _benchmark(() => loadIkv200(ruFile200, false), 2, 1);
      await _benchmark(() => loadIkv100(ruFile100, false), 2, 1);

      print('Testing...');
      var currentRuSense =
          await _benchmark(() => loadCurr(ruFileCurr, false), 6, 1);
      var ikv200RuSense =
          await _benchmark(() => loadIkv200(ruFile200, false), 6, 1);
      var ikv100RuSense =
          await _benchmark(() => loadIkv100(ruFile100, false), 6, 1);
      var currentEnSense =
          await _benchmark(() => loadCurr(enFileCurr, false), 6, 1);
      var ikv200EnSense =
          await _benchmark(() => loadIkv200(enFile200, false), 6, 1);
      var ikv100EnSense =
          await _benchmark(() => loadIkv100(enFile100, false), 6, 1);

      currentRuSense.prnt('CURR RU, CASE-SENS', true);
      ikv200RuSense.prnt('I200 RU, CASE-SENS', true);
      ikv100RuSense.prnt('I100 RU, CASE-SENS', true);
      currentEnSense.prnt('CURR EN, CASE-SENS', true);
      ikv200EnSense.prnt('I200 EN, CASE-SENS', true);
      ikv100EnSense.prnt('I100 EN, CASE-SENS', true);

      expect(
          (currentRuSense.avgMicro - ikv100RuSense.avgMicro) *
                  100 /
                  ikv100RuSense.avgMicro <
              15,
          true);
      expect(
          (currentEnSense.avgMicro - ikv100EnSense.avgMicro) *
                  100 /
                  ikv100EnSense.avgMicro <
              35,
          true); // Tradeoff in newer versions, slower EN (which is still very fast) while much faster RU (which was 5-6 tims slower than EN)

      expect(
          (currentRuSense.avgMicro - ikv200RuSense.avgMicro) *
                  100 /
                  ikv200RuSense.avgMicro <
              15,
          true);
      expect(
          (currentEnSense.avgMicro - ikv200EnSense.avgMicro) *
                  100 /
                  ikv200EnSense.avgMicro <
              15,
          true);
    }, skip: skippLongTests);

    // this one is very slow
    test('Case-Insensitive, Current version not slower to load in isolate pool',
        () async {
      print('Warming up');

      var poolCurr = IsolatePool(1);
      await poolCurr.start();
      var pool200 = ikv200.IsolatePool(1);
      await pool200.start();
      var pool100 = ikv100.IsolatePool(1);
      await pool100.start();

      await _benchmark(() => loadCurr(ruFileCurr, true, poolCurr), 2, 1);
      await _benchmark(() => loadIkv200(ruFile200, true, pool200), 2, 1);
      await _benchmark(() => loadIkv100(ruFile100, true, pool100), 2, 1);

      print('Testing...');

      var ikv200RuInsense =
          await _benchmark(() => loadIkv200(ruFile200, true, pool200), 3, 1);
      var ikv100RuInsense =
          await _benchmark(() => loadIkv100(ruFile100, true, pool100), 3, 1);

      var currentRuInsense =
          await _benchmark(() => loadCurr(ruFileCurr, true, poolCurr), 3, 1);

      var currentEnInsense =
          await _benchmark(() => loadCurr(enFileCurr, true, poolCurr), 3, 1);
      var ikv200EnInsense =
          await _benchmark(() => loadIkv200(enFile200, true, pool200), 3, 1);
      var ikv100EnInsense =
          await _benchmark(() => loadIkv100(enFile100, true, pool100), 3, 1);

      currentRuInsense.prnt('CURR RU, CASE-INSE', true);
      ikv200RuInsense.prnt('I200 RU, CASE-INSE', true);
      ikv100RuInsense.prnt('I100 RU, CASE-INSE', true);

      currentEnInsense.prnt('CURR EN, CASE-INSE', true);
      ikv200EnInsense.prnt('I200 EN, CASE-INSE', true);
      ikv100EnInsense.prnt('I100 EN, CASE-INSE', true);

      expect(
          (currentRuInsense.avgMicro - ikv100RuInsense.avgMicro) *
                  100 /
                  ikv100RuInsense.avgMicro <
              10,
          true);
      expect(
          (currentEnInsense.avgMicro - ikv100EnInsense.avgMicro) *
                  100 /
                  ikv100EnInsense.avgMicro <
              10,
          true);

      expect(
          (currentRuInsense.avgMicro - ikv200RuInsense.avgMicro) *
                  100 /
                  ikv200RuInsense.avgMicro <
              10,
          true);
      expect(
          (currentEnInsense.avgMicro - ikv200EnInsense.avgMicro) *
                  100 /
                  ikv200EnInsense.avgMicro <
              10,
          true);
    }, timeout: Timeout(Duration(seconds: 160)), skip: skippLongTests);
  });

  group('Trying out certain patterns for performance', () {
    // ORG   - AVG:746, MIN:419, MAX:1845
    // CUS   - AVG:777, MIN:484, MAX:2149
    // CUS2  - AVG:382, MIN:347, MAX:643

    // ORG   - AVG:782, MIN:442, MAX:1859
    // CUS   - AVG:845, MIN:536, MAX:2544
    // CUS2  - AVG:401, MIN:364, MAX:676

    test('_fixOutOfOrder', () async {
      var keys = testMap.keys.toList();

      List<String> orig() {
        var kk = <String>[];
        for (var k in keys) {
          var s =
              k.replaceAll('ё', 'е').replaceAll('і', 'и').replaceAll('ў', 'у');
          kk.add(s);
        }
        return kk;
      }

      var c1 = 'ё'.codeUnits[0];
      var cc1 = 'е'.codeUnits[0];
      var c2 = 'і'.codeUnits[0];
      var cc2 = 'и'.codeUnits[0];
      var c3 = 'ў'.codeUnits[0];
      var cc3 = 'у'.codeUnits[0];

      List<String> codeUnits() {
        var kk = <String>[];
        for (var k in keys) {
          var cus = <int>[];
          for (var i in k.codeUnits) {
            if (i == c1) {
              cus.add(cc1);
            } else if (i == c2) {
              cus.add(cc2);
            } else if (i == c3) {
              cus.add(cc3);
            } else {
              cus.add(i);
            }
          }
          var s = String.fromCharCodes(cus);
          kk.add(s);
        }
        return kk;
      }

      List<String> codeUnits2() {
        var kk = List<String>.generate(keys.length, (i) {
          var k = keys[i];
          var cus = List<int>.generate(k.length, (index) {
            var i = k.codeUnitAt(index);
            if (i == c1) {
              return cc1;
            } else if (i == c2) {
              return cc2;
            } else if (i == c3) {
              return cc3;
            } else {
              return i;
            }
          }, growable: false);
          var s = String.fromCharCodes(cus);
          return s;
        }, growable: false);
        return kk;
      }

      List<String> uint16() {
        var kk = List<String>.generate(keys.length, (i) {
          var k = keys[i];
          var cus = Uint16List(k.length);

          for (var i = 0; i < k.length; i++) {
            if (k.codeUnits[i] == c1) {
              cus[i] = cc1;
            } else if (k.codeUnits[i] == c2) {
              cus[i] = cc2;
            } else if (k.codeUnits[i] == c3) {
              cus[i] = cc3;
            } else {
              cus[i] = k.codeUnits[i];
            }
          }

          var s = String.fromCharCodes(cus);
          return s;
        }, growable: false);
        return kk;
      }

      List<String> uint32() {
        var kk = List<String>.generate(keys.length, (i) {
          var k = keys[i];
          var cus = Uint32List(k.length);

          for (var i = 0; i < k.length; i++) {
            if (k.codeUnits[i] == c1) {
              cus[i] = cc1;
            } else if (k.codeUnits[i] == c2) {
              cus[i] = cc2;
            } else if (k.codeUnits[i] == c3) {
              cus[i] = cc3;
            } else {
              cus[i] = k.codeUnits[i];
            }
          }

          var s = String.fromCharCodes(cus);
          return s;
        }, growable: false);
        return kk;
      }

      List<String> uint16uint16() {
        var kk = List<String>.generate(keys.length, (i) {
          var k = keys[i];
          var cu = Uint16List.fromList(k.codeUnits);
          var cus = Uint16List(k.length);

          for (var i = 0; i < k.length; i++) {
            if (cu[i] == c1) {
              cus[i] = cc1;
            } else if (cu[i] == c2) {
              cus[i] = cc2;
            } else if (cu[i] == c3) {
              cus[i] = cc3;
            } else {
              cus[i] = cu[i];
            }
          }

          var s = String.fromCharCodes(cus);
          return s;
        }, growable: false);
        return kk;
      }

      List<String> justUint16() {
        var kk = List<String>.generate(keys.length, (i) {
          var k = keys[i];
          var cu = Uint16List.fromList(k.codeUnits);

          for (var i = 0; i < k.length; i++) {
            if (cu[i] == c1) {
              cu[i] = cc1;
            } else if (cu[i] == c2) {
              cu[i] = cc2;
            } else if (cu[i] == c3) {
              cu[i] = cc3;
            }
          }

          var s = String.fromCharCodes(cu);
          return s;
        }, growable: false);
        return kk;
      }

      List<String> contains() {
        var kk = List<String>.generate(keys.length, (i) {
          var s = keys[i];
          if (s.contains('ё')) s = s.replaceAll('ё', 'е');
          if (s.contains('і')) s = s.replaceAll('і', 'и');
          if (s.contains('ў')) s = s.replaceAll('ў', 'у');
          return s;
        }, growable: false);
        return kk;
      }

      var org = _benchmarkSync(orig);
      var cus = _benchmarkSync(codeUnits);
      var cus2 = _benchmarkSync(codeUnits2);
      var int16 = _benchmarkSync(uint16);
      var int32 = _benchmarkSync(uint32);
      var int1616 = _benchmarkSync(uint16uint16);
      var just16 = _benchmarkSync(justUint16);
      var cont = _benchmarkSync(contains);

      org.prnt('ORG  ');
      cus.prnt('CUS  ');
      cus2.prnt('CUS2 ');
      int16.prnt('INT16 ');
      int32.prnt('INT32 ');
      int1616.prnt('INT1616 ');
      just16.prnt('JUST16');
      cont.prnt('CONT');
    });

    test('UTF8 vs UTF16 decoding', () async {
      //var keys = testMap.keys;

      // var ikv = IkvPack(ruFile, false);
      var ikv = await IkvPack.load(enFileCurr, false);
      var keys = ikv.keys;

// RU
// utf8  length: 58039684
// utf16 length: 60634352
// UTF8   - AVG:655206.2, MIN:597968.0, MAX:760960.0
// UTF16  - AVG:487054.4, MIN:386144.0, MAX:709820.0
// UTF16L - AVG:827344.6, MIN:759329.0, MAX:923210.0

// EN
// utf8  length: 18476279
// utf16 length: 36952558
// UTF8   - AVG:179354.2, MIN:126940.0, MAX:277340.0
// UTF16  - AVG:161621.5, MIN:123747.0, MAX:225360.0
// UTF16L - AVG:406316.8, MIN:377385.0, MAX:464356.0

      var utf8bytes = <Uint8List>[];
      var utf16bytes = <Uint16List>[];
      var utf8length = 0;
      var utf16length = 0;

      for (var k in keys) {
        utf8bytes.add(Uint8List.fromList(utf8.encode(k)));
        utf8length += utf8bytes.last.length;
        utf16bytes.add(Uint16List.fromList(k.codeUnits));
        utf16length += utf16bytes.last.length * 2;
      }

      print('utf8  length: $utf8length');
      print('utf16 length: $utf16length');

      var decoder = Utf8Decoder(allowMalformed: true);

      List<String> utf8decode() {
        var k = <String>[];
        for (var b in utf8bytes) {
          var s = decoder.convert(b);
          k.add(s);
        }
        return k;
      }

      List<String> utf16decode() {
        var k = <String>[];
        for (var b in utf16bytes) {
          var s = String.fromCharCodes(b);
          k.add(s);
        }
        return k;
      }

      var utf16list = utf16bytes.map((i) => i.toList());

      List<String> utf16ListDecode() {
        var k = <String>[];
        for (var b in utf16list) {
          var s = String.fromCharCodes(b);
          k.add(s);
        }
        return k;
      }

      var utf8bench = _benchmarkSync(utf8decode);
      var utf16bench = _benchmarkSync(utf16decode);
      var utf16ListBench = _benchmarkSync(utf16ListDecode);
      utf8bench.prnt('UTF8  ');
      utf16bench.prnt('UTF16 ');
      utf16ListBench.prnt('UTF16L');
    });
  }, skip: true);

  test('Utf8 decode short vs long string', () async {
    //var keys = testMap.keys;

    // var ikv = IkvPack(ruFile, false);
    var ikv = await IkvPack.load(ruFileCurr, false);
    var keys = ikv.keys;

    var singleBytes = <Uint8List>[];
    var quadraBytes = <Uint8List>[];

    var i = 0;
    var bb = BytesBuilder();

    var nl = '\n'.codeUnits[0];

    for (var k in keys) {
      var x = Uint8List.fromList(utf8.encode(k));
      singleBytes.add(x);
      i++;
      bb.add(Uint8List.fromList(utf8.encode(k)));
      if (i != 4) bb.addByte(nl);
      if (i == 4) {
        i = 0;
        quadraBytes.add(bb.takeBytes());
      }
    }

    if (bb.length > 0) {
      quadraBytes.add(bb.takeBytes());
    }

    var decoder = Utf8Decoder(allowMalformed: true);

    List<String> single() {
      var k = <String>[];
      for (var b in singleBytes) {
        var s = decoder.convert(b);
        k.add(s);
      }
      return k;
    }

    List<String> quadra() {
      var k = <String>[];
      for (var b in quadraBytes) {
        var start = 0;
        var i = 0;
        for (i = i; i < b.length; i++) {
          if (b[i] == nl) {
            var s = decoder.convert(Uint8List.view(b.buffer, start, i - start));
            k.add(s);
            start = i + 1;
          }
        }
        var s = decoder.convert(Uint8List.view(b.buffer, start, i - start));
        k.add(s);
      }
      return k;
    }

    var singleBench = _benchmarkSync(single);
    var quadraBench = _benchmarkSync(quadra);
    singleBench.prnt('Single', true);
    quadraBench.prnt('Quadra', true);
  }, skip: true);

  test('Ikv.fromMap sync vs async', () async {
    void fromMap() {
      var _ = IkvPack.fromMap(testMap);
    }

    void fromMapAsync() async {
      var _ = await IkvPack.buildFromMapAsync(testMap);
    }

    var sn = _benchmarkSync(fromMap);
    var asn = _benchmarkSync(fromMapAsync);

    sn.prnt('Sync', true);
    asn.prnt('Async', true);

    expect((sn.avgMicro - asn.avgMicro).abs() / sn.avgMicro < 0.125, true);
  }, skip: skippLongTests);
}

class _Benchmark {
  final double minMicro;
  final double maxMicro;
  final double avgMicro;
  dynamic result;

  _Benchmark(this.minMicro, this.maxMicro, this.avgMicro);

  void prnt(String title, [bool miliseconds = false]) {
    print(title +
        ' - AVG:${(miliseconds ? avgMicro / 1000 : avgMicro).toStringAsFixed(1)}, '
            'MIN:${(miliseconds ? minMicro / 1000 : minMicro).toStringAsFixed(1)}, '
            'MAX:${(miliseconds ? maxMicro / 1000 : maxMicro).toStringAsFixed(1)}');
  }
}

Future<_Benchmark> _benchmark(Future Function() test,
    [int n = 20, int warmupN = 3]) async {
  var min = -1;
  var max = -1;
  // ignore: omit_local_variable_types
  double avg = 0;
  var sw = Stopwatch();
  dynamic result;

  for (var i = 0 - warmupN; i < n; i++) {
    if (i < 0) {
      await test();
    } else {
      sw.start();
      result = await test();
      sw.stop();

      if (min == -1) {
        min = max = sw.elapsedMicroseconds;
      } else {
        if (sw.elapsedMicroseconds > max) max = sw.elapsedMicroseconds;
        if (sw.elapsedMicroseconds < min) min = sw.elapsedMicroseconds;
      }

      avg += sw.elapsedMicroseconds;

      sw.reset();
    }
  }

  avg /= n;

  var b = _Benchmark(min.toDouble(), max.toDouble(), avg)..result = result;

  return b;
}

_Benchmark _benchmarkSync(Function test, [int n = 20, int warmupN = 3]) {
  var min = -1;
  var max = -1;
  // ignore: omit_local_variable_types
  double avg = 0;
  var sw = Stopwatch();
  dynamic result;

  for (var i = 0 - warmupN; i < n; i++) {
    if (i < 0) {
      test();
    } else {
      sw.start();
      result = test();
      sw.stop();

      if (min == -1) {
        min = max = sw.elapsedMicroseconds;
      } else {
        if (sw.elapsedMicroseconds > max) max = sw.elapsedMicroseconds;
        if (sw.elapsedMicroseconds < min) min = sw.elapsedMicroseconds;
      }

      avg += sw.elapsedMicroseconds;

      sw.reset();
    }
  }

  avg /= n;

  var b = _Benchmark(min.toDouble(), max.toDouble(), avg)..result = result;

  return b;
}
