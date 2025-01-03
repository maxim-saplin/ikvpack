import 'dart:convert';

import 'package:archive/archive_io.dart';
import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/ikvpack_core.dart';
import 'package:test/test.dart';

import 'test_bytes.dart';

late IkvPack _ikv;

void setIkv(IkvPack ikv) {
  _ikv = ikv;
}

IkvPack get ikv => _ikv;

void runCaseInvariantTests([bool proxySkip = false]) {
  test('Can serach key by index', () {
    var k = _ikv.keys[0];
    expect(k, '0AA');
    k = _ikv.keys[5];
    expect(k, 'Aaron Burr');
  }, skip: proxySkip);

  test('Can get value by index', () async {
    var v = await _ikv.getValueAt(4);
    expect(
        v.startsWith(
            '<div><i>adjective</i></div><div>being six more than thirty'),
        true);
    v = await _ikv.getValueAt(5);
    expect(
        v.startsWith(
            '<div><i>noun</i></div><div>United States politician who served'),
        true);
  });

  test('Can get a value through getValues())', () async {
    var v = await _ikv.getValues('зараць');
    expect(v[0], '<div>вспахать</div>');
  });

  test('Getting raw range works', () async {
    var range = await _ikv.getRangeRaw(1, 11);
    expect(range.length, 11);
    var x = range.entries.skip(4).first;
    expect(x.key, 'Aaron Burr');
  });

  test('Getting range works', () async {
    var range = await _ikv.getRange(1, 11);
    expect(range.length, 11);
    var x = range.entries.skip(4).first;
    expect(x.key, 'Aaron Burr');
    expect(x.value,
        '<div><i>noun</i></div><div>United States politician who served as vice president under Jefferson; he mortally wounded his political rival Alexander Hamilton in a duel and fled south <i>(1756-1836)</i></div><div><span>•</span> <i>Syn</i>: ↑<a href=Burr>Burr</a></div><div><span>•</span> <i>Instance Hypernyms</i>: ↑<a href=politician>politician</a>, ↑<a href=politico>politico</a>, ↑<a href=pol>pol</a>, ↑<a href=political leader>political leader</a></div>');
  });

  test('Can get value by key (getValue)', () async {
    var v = await _ikv['зараць'];
    expect(v, '<div>вспахать</div>');
    v = await _ikv.getValue('зараць');
    expect(v, '<div>вспахать</div>');
  });

  test('Can get raw uncompressed value via index', () async {
    var v = await _ikv.getValueRawCompressed('зараць');
    expect(v.isNotEmpty, true);
    var index = _ikv.indexOf('зараць');
    var uncompressed = '<div>вспахать</div>';
    var utf = utf8.encode(uncompressed);
    var compressed = Deflate(utf).getBytes();
    v = await _ikv.getValueRawCompressedAt(index);
    expect(v.isNotEmpty, true);
    expect(compressed.length, v.length);
    expect(compressed[0], v[0]);
    expect(compressed[compressed.length - 1], v[compressed.length - 1]);
  }, skip: proxySkip);

  test('Can get raw uncompressed value via key', () async {
    var v = await _ikv.getValueRawCompressed('зараць');
    expect(v.isNotEmpty, true);
    var uncompressed = '<div>вспахать</div>';
    var utf = utf8.encode(uncompressed);
    var compressed = Deflate(utf).getBytes();
    v = await _ikv.getValueRawCompressed('зараць');
    expect(v.isNotEmpty, true);
    expect(compressed.length, v.length);
    expect(compressed[0], v[0]);
    expect(compressed[compressed.length - 1], v[compressed.length - 1]);
  });

  test('keysStartingWith() finds the key', () async {
    var keys = await _ikv.keysStartingWith('aerosol', 3);
    expect(keys[0].original, 'aerosol bomb');
  });

  test('Key in keysStartingWith() is trimmed', () async {
    var keys = await _ikv.keysStartingWith(' Acer ', 1);
    expect(keys.length, 1);
  });

  test('Non existing keys return empty result', () async {
    expect(_ikv.containsKey('wewer'), false);
    expect(await _ikv['wewer'], '');
    expect((await _ikv.getValueRawCompressed('wewer')).isEmpty, true);
  }, skip: proxySkip);
}

// Tests again IkvProxy with IkvPack leaving in isolate do not support all IkvPack APIs and are skipped
void runCaseInsensitiveTests([bool proxySkip = false]) {
  test('Out of order keys are fixed (ё isnt below я)', () {
    var k = _ikv.keys[_ikv.keys.length - 1];
    expect(k, 'яскравасьць');
  }, skip: proxySkip);

  test('Case-insensitive search by key works', () async {
    var v = await _ikv['afrikaans'];
    expect(
        v.startsWith(
            '<div><b>I</b></div><div><i>noun</i></div><div>an official language of the'),
        true);
  });

  test('Can get multiple value by case insensitive key (getValues)', () async {
    var v = await _ikv.getValues('зараць');
    expect(v[0], '<div>вспахать</div>');

    v = await _ikv.getValues('lucid');
    expect(v.length, 2);
    expect(v.toSet(),
        {'<div>lower case lucid </div>', '<div>upper case lucid </div>'});

    v = await _ikv.getValues('0aA');
    expect(v.length, 4);
    expect(v.toSet(), {'0AA', '0Aa', '0aA', '0aa'});

    v = await _ikv.getValues('bbb');
    expect(v.length, 3);
    expect(v.toSet(), {'BBB', 'Bbb', 'BbB'});
  });

  test('Keys are sorted', () {
    expect(_ikv.keys[_ikv.keys.length - 8] == 'эліпс', true);
    expect(_ikv.keys[_ikv.keys.length - 3] == 'юродзівасьць', true);
    expect(_ikv.keys[_ikv.keys.length - 1] == 'яскравасьць', true);
  }, skip: proxySkip);

  test('Key keysStartingWith() limits the result', () async {
    var keys = await _ikv.keysStartingWith('an', 3);
    expect(keys.length, 3);
  });

  test('Key keysStartingWith() conducts case-insensitive search', () async {
    var keys = await _ikv.keysStartingWith('ЗЬ');
    expect(keys.length, 6);
  });

  test(
      'Key keysStartingWith() search cases insesitive keys but returns original keys',
      () async {
    var keys = await _ikv.keysStartingWith('неглижэ');
    expect(keys[0].original, 'негліжэ');
  });

  test('"и" and "i" subsitute wroks when looking up Belarusian words',
      () async {
    var keys = await _ikv.keysStartingWith('ихтыёл');
    expect(keys.length, 1);
  });

  test('"и" and "i" subsitute wroks when getting value by original key',
      () async {
    var keys = await _ikv.keysStartingWith('ихтыёл');
    expect(keys.length, 1);
    var val = await _ikv[keys[0].original];
    expect(val, '<div>ихтиол</div>');
  });

  test('Consolidated keysStartingWith works on case-insensitive keys',
      () async {
    var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
    var ik = IkvPack.fromMap(m);
    var ikvs = [_ikv, _ikv, ik];

    var keys = await IkvPack.consolidatedKeysStartingWith(ikvs, 'зьнізіць');

    expect(keys.length, 1);
    expect(keys[0], 'зьнізіць');

    keys = await IkvPack.consolidatedKeysStartingWith(ikvs, 'b', 10);
    expect(keys.length, 10);
  });

  test('Consolidated keysStartingWith works returns properly sorted keys',
      () async {
    var ikvs = [_ikv, _ikv];

    var keys = await IkvPack.consolidatedKeysStartingWith(ikvs, 'a');

    expect(keys[0], 'Aaron Burr');
    expect(keys[1], 'ablutionary');
    expect(keys[2], 'abstentious');
    expect(keys[3], 'accentual system');
    expect(keys[4], 'Acer macrophyllum');
    expect(keys[5], 'acneiform');
    expect(keys[6], 'actinic ray');
  });

  test(
      'Can get value/values by keys returned via keysStartingWith() (getValue/getValues)',
      () async {
    var k = await _ikv.keysStartingWith('фі');
    expect(k.length, 2);
    expect(k[0].original, 'фіёрд');
    expect(k[1].original, 'філялягічны');

    var v = await _ikv.getValue(k[0].original);
    expect(v, '<div>фиорд</div>');
    v = await _ikv.getValue(k[1].original);
    expect(v, '<div><i>см.</i> <a href=філалагічны>філалагічны</a></div>');

    var v2 = await _ikv.getValues(k[0].original);
    expect(v2[0], '<div>фиорд</div>');
    v2 = await _ikv.getValues(k[1].original);
    expect(v2[0], '<div><i>см.</i> <a href=філалагічны>філалагічны</a></div>');

    k = await _ikv.keysStartingWith('эў');
    expect(k.length, 1);
    expect(k[0].original, 'эўфанія');

    v = await _ikv.getValue(k[0].original);
    expect(v, '<div>эвфония</div>');
    v2 = await _ikv.getValues(k[0].original);
    expect(v2[0], '<div>эвфония</div>');
  });

  test(
      'Consolidated keysStartingWith() doesn\'t fail for exact single key (bug fix)',
      () async {
    var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
    var ik = IkvPack.fromMap(m);
    var ikvs = [_ikv, ik];

    var keys =
        await IkvPack.consolidatedKeysStartingWith(ikvs, 'north by east');
    expect(keys.length, 1);
  });

  test('Consolidated keysStartingWith returns original keys', () async {
    var m = <String, String>{'': '', 'Wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
    var ik = IkvPack.fromMap(m);
    var ikvs = [_ikv, _ikv, ik];

    var keys = await IkvPack.consolidatedKeysStartingWith(ikvs, 'w');
    expect(keys.contains('Wew'), true);
  });
}

void runStorageInvariantTests() {
  test('Same data is read back', () async {
    var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
    var ik = IkvPack.fromMap(m);

    expect(ik.length, 3);
    expect(await ik['a'], 'aaa');
    expect(await ik['b'], 'bbb');
    expect(await ik['c'], 'ccc');

    await ik.saveTo('tmp/test.dat');
    ik = await IkvPack.load('tmp/test.dat');

    expect(ik.length, 3);
    expect(await ik['a'], 'aaa');
    expect(await ik['b'], 'bbb');
    expect(await ik['c'], 'ccc');
  });

  test('Data is read correctly (w. IkvMap.fromBytes)', () async {
    var ik = IkvPack.fromBytes(testBytes.buffer.asByteData());

    expect(ik.length, 1445);
    expect(await ik['nonechoic'],
        '<div><i>adjective</i></div><div>not echoic or imitative of sound</div><div><span>•</span> <i>Ant</i>: ↑<a href=echoic>echoic</a></div>');

    await ik.saveTo('tmp/test.dat');
    ik = await IkvPack.load('tmp/test.dat');

    expect(ik.length, 1445);
    expect(await ik['nonechoic'],
        '<div><i>adjective</i></div><div>not echoic or imitative of sound</div><div><span>•</span> <i>Ant</i>: ↑<a href=echoic>echoic</a></div>');
  });

  test('Saving to eixting path rewrites what\'s there', () async {
    var m = <String, String>{'a': 'aaa', 'b': 'bbb', 'c': 'ccc'};
    var ik = IkvPack.fromMap(m);
    print('Map created');
    await ik.saveTo('tmp/test.dat');
    print('Saved');
    ik = await IkvPack.load('tmp/test.dat');
    print('Loaded');
    expect(ik.length, 3);

    m = <String, String>{'z': 'zzz', 'y': 'yyyy'};
    ik = IkvPack.fromMap(m);
    print('Map created');
    await ik.saveTo('tmp/test.dat');
    print('Saved');
    ik = await IkvPack.load('tmp/test.dat');
    print('Loaded');
    expect(ik.length, 2);
  });
}
