import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'testMap.dart';
import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack_100/ikvpack.dart' as ikv100;

const ruFile = 'test/performance_data/RU_EN Multitran vol.2.dikt';
const enFile = 'test/performance_data/EN_RU Multitran vol.2.dikt';

void main() async {
  group('Real file tests', () {
    test('Current version not slower than 1.0.0', () {
      // var ikv = IkvPack(ruFile, false);
      // var stats = ikv.getStats();
      // ikv = IkvPack(enFile, false);
      // stats = ikv.getStats();

      int loadCurrent(String path, bool keysCaseInsensitive) {
        var ikv = IkvPack(path, keysCaseInsensitive);
        return ikv.length;
      }

      int loadIkv100(String path, bool keysCaseInsensitive) {
        var ikv = ikv100.IkvPack(path, keysCaseInsensitive);
        return ikv.length;
      }

      // var currentRuSense = _benchmark(() => loadCurrent(ruFile, false), 1, 2);
      // //var currentEnSense = _benchmark(() => loadCurrent(enFile, false), 1, 2);

      // currentRuSense.prnt('CURR RU, CASE-SENS', true);
      // //currentEnSense.prnt('CURR EN, CASE-SENS', true);

      // var currentRuInsense = _benchmark(() => loadCurrent(ruFile, true), 6, 1);
      // var ikv100RuInsense = _benchmark(() => loadIkv100(ruFile, true), 6, 1);
      // var currentEnInsense = _benchmark(() => loadCurrent(enFile, true), 6, 1);
      // var ikv100EnInsense = _benchmark(() => loadIkv100(enFile, true), 6, 1);

      var currentRuSense = _benchmark(() => loadCurrent(ruFile, false), 6, 1);
      var ikv100RuSense = _benchmark(() => loadIkv100(ruFile, false), 6, 1);
      var currentEnSense = _benchmark(() => loadCurrent(enFile, false), 6, 1);
      var ikv100EnSense = _benchmark(() => loadIkv100(enFile, false), 6, 1);

      // currentRuInsense.prnt('CURR RU, CASE-INSE', true);
      // ikv100RuInsense.prnt('I100 RU, CASE-INSE', true);
      // currentEnInsense.prnt('CURR EN, CASE-INSE', true);
      // ikv100EnInsense.prnt('I100 EN, CASE-INSE', true);

      currentRuSense.prnt('CURR RU, CASE-SENS', true);
      ikv100RuSense.prnt('I100 RU, CASE-SENS', true);
      currentEnSense.prnt('CURR EN, CASE-SENS', true);
      ikv100EnSense.prnt('I100 EN, CASE-SENS', true);
    }, skip: true);
  });

  group('Trying out certain patterns for performance', () {
    // ORG   - AVG:746, MIN:419, MAX:1845
    // CUS   - AVG:777, MIN:484, MAX:2149
    // CUS2  - AVG:382, MIN:347, MAX:643

    // ORG   - AVG:782, MIN:442, MAX:1859
    // CUS   - AVG:845, MIN:536, MAX:2544
    // CUS2  - AVG:401, MIN:364, MAX:676

    test('_fixOutOfOrder', () {
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

      List<String> codeUnits() {
        var kk = <String>[];
        var c1 = 'ё'.codeUnits[0];
        var cc1 = 'е'.codeUnits[0];
        var c2 = 'і'.codeUnits[0];
        var cc2 = 'и'.codeUnits[0];
        var c3 = 'ў'.codeUnits[0];
        var cc3 = 'у'.codeUnits[0];
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
        var c1 = 'ё'.codeUnits[0];
        var cc1 = 'е'.codeUnits[0];
        var c2 = 'і'.codeUnits[0];
        var cc2 = 'и'.codeUnits[0];
        var c3 = 'ў'.codeUnits[0];
        var cc3 = 'у'.codeUnits[0];

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

      var org = _benchmark(orig);
      var cus = _benchmark(codeUnits);
      var cus2 = _benchmark(codeUnits2);

      org.prnt('ORG  ');
      cus.prnt('CUS  ');
      cus2.prnt('CUS2 ');
    });

    test('UTF8 vs UTF16 decoding', () {
      //var keys = testMap.keys;

      // var ikv = IkvPack(ruFile, false);
      var ikv = IkvPack(enFile, false);
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

      print('utf8  length: ${utf8length}');
      print('utf16 length: ${utf16length}');

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

      var utf8bench = _benchmark(utf8decode);
      var utf16bench = _benchmark(utf16decode);
      var utf16ListBench = _benchmark(utf16ListDecode);
      utf8bench.prnt('UTF8  ');
      utf16bench.prnt('UTF16 ');
      utf16ListBench.prnt('UTF16L');
    });
  }, skip: false);
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

_Benchmark _benchmark(Function test, [int n = 20, int warmupN = 3]) {
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
