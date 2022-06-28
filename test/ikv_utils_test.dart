@TestOn('vm')

import 'dart:io';
import 'package:ikvpack/ikvpack.dart';
import 'package:test/test.dart';

void main() async {
  void deleteFiles([bool all = true]) {
    var files = [
      'tmp/ikv1.ikv',
      'tmp/ikv2.ikv',
      'tmp/ikv3.ikv',
      if (all) 'tmp/ikv.mikv',
      if (all) 'tmp/ikv.part1.ikv',
      if (all) 'tmp/ikv.part2.ikv',
      if (all) 'tmp/ikv.part3.ikv',
    ];
    for (var f in files) {
      var ff = File(f);
      if (ff.existsSync()) {
        ff.deleteSync();
      }
    }
  }

  setUpAll(() {
    deleteFiles();
    Directory('tmp').createSync();
  });

  tearDownAll(() {
    //deleteFiles();
    Directory('tmp').deleteSync(recursive: true);
  });

  test(
      'Ikv is saved to files, merged, unmerged and correct Ikv data is read back',
      () async {
    var m1 = <String, String>{'a': 'a'};
    var m2 = <String, String>{'aa': 'aa', 'bb': 'bb'};
    var m3 = <String, String>{'aaa': 'aaa', 'bbb': 'bbb', 'ccc': 'ccc'};
    for (var i = 0; i < 1000; i++) {
      m3[i.toString()] = 'aaa' * 3;
    } // need one file larger than 4kb
    var ikv1 = IkvPack.fromMap(m1);
    var ikv2 = IkvPack.fromMap(m2);
    var ikv3 = IkvPack.fromMap(m3);

    await ikv1.saveTo('tmp/ikv1.ikv');
    await ikv2.saveTo('tmp/ikv2.ikv');
    await ikv3.saveTo('tmp/ikv3.ikv');

    putIntoSingleFile([
      'tmp/ikv1.ikv',
      'tmp/ikv2.ikv',
      'tmp/ikv3.ikv',
    ], 'tmp/ikv.mikv');

    expect(File('tmp/ikv.mikv').existsSync(), true);
    deleteFiles(false);

    extractFromSingleFile('tmp/ikv.mikv', 'tmp');

    ikv1 = await IkvPack.load('tmp/ikv.part1.ikv');
    ikv2 = await IkvPack.load('tmp/ikv.part2.ikv');
    ikv3 = await IkvPack.load('tmp/ikv.part3.ikv');

    expect(ikv1.length, 1);
    expect(ikv2.length, 2);
    expect(ikv3.length, 1003);
    expect(await ikv1['a'], 'a');
    expect(await ikv2['bb'], 'bb');
    expect(await ikv3['ccc'], 'ccc');
  });

  test('Empty files are correctly handled', () async {
    File('tmp/ikv1.ikv').createSync();
    File('tmp/ikv2.ikv').createSync();
    File('tmp/ikv3.ikv').createSync();

    expect(File('tmp/ikv1.ikv').lengthSync(), 0);
    expect(File('tmp/ikv2.ikv').lengthSync(), 0);
    expect(File('tmp/ikv3.ikv').lengthSync(), 0);

    putIntoSingleFile([
      'tmp/ikv1.ikv',
      'tmp/ikv2.ikv',
      'tmp/ikv3.ikv',
    ], 'tmp/ikv.mikv');

    expect(File('tmp/ikv.mikv').existsSync(), true);

    extractFromSingleFile('tmp/ikv.mikv', 'tmp');

    expect(File('tmp/ikv.part1.ikv').lengthSync(), 0);
    expect(File('tmp/ikv.part1.ikv').lengthSync(), 0);
    expect(File('tmp/ikv.part1.ikv').lengthSync(), 0);
  });

  test(
      'Empty files are correctly handled and one non-emty are correctly handled',
      () async {
    File('tmp/ikv1.ikv').createSync();
    var m2 = <String, String>{'aa': 'aa', 'bb': 'bb'};
    var ikv2 = IkvPack.fromMap(m2);
    await ikv2.saveTo('tmp/ikv2.ikv');

    expect(File('tmp/ikv1.ikv').lengthSync(), 0);

    putIntoSingleFile([
      'tmp/ikv1.ikv',
      'tmp/ikv2.ikv',
    ], 'tmp/ikv.mikv');

    expect(File('tmp/ikv.mikv').existsSync(), true);

    extractFromSingleFile('tmp/ikv.mikv', 'tmp');

    expect(File('tmp/ikv.part1.ikv').lengthSync(), 0);

    ikv2 = await IkvPack.load('tmp/ikv.part2.ikv');

    expect(ikv2.length, 2);
    expect(await ikv2['bb'], 'bb');
  });
}
