import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'testMap.dart';

void main() async {
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
      var keys = testMap.keys;

      var utf8bytes = <Uint8List>[];
      var utf16bytes = <Uint8List>[];
      var utf8length = 0;
      var utf16length = 0;

      for (var k in keys) {
        utf8bytes.add(Uint8List.fromList(utf8.encode(k)));
        utf8length += utf8bytes.last.length;
        utf16bytes.add(Uint16List.fromList(k.codeUnits).buffer.asUint8List());
        utf16length += utf16bytes.last.length;
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
          var s = String.fromCharCodes(b.buffer.asUint16List().toList());
          k.add(s);
        }
        return k;
      }

      var utf16list = utf16bytes.map((i) => i.buffer.asUint16List().toList());

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
  });
}

class _Benchmark {
  final int minMicro;
  final int maxMicro;
  final int avgMicro;
  dynamic result;

  _Benchmark(this.minMicro, this.maxMicro, this.avgMicro);

  void prnt(String title) {
    print(title + ' - AVG:${avgMicro}, MIN:${minMicro}, MAX:${maxMicro}');
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

  var b = _Benchmark(min, max, avg.round())..result = result;

  return b;
}
