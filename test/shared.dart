import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/ikvpack.dart';
import 'package:test/test.dart';

late IkvPack _ikv;

void setIkv(IkvPack ikv) {
  _ikv = ikv;
}

IkvPack get ikv => _ikv;

void runCaseInvariantTests() {
  test('Can serach key by index', () {
    var k = _ikv.keys[0];
    expect(k, '36');
    k = _ikv.keys[1];
    expect(k, 'Aaron Burr');
  });

  test('Can get value by index', () async {
    var v = await _ikv.valueAt(0);
    expect(
        v.startsWith(
            '<div><i>adjective</i></div><div>being six more than thirty'),
        true);
    v = await _ikv.valueAt(1);
    expect(
        v.startsWith(
            '<div><i>noun</i></div><div>United States politician who served'),
        true);
  });

  test('Getting raw range works', () async {
    var range = await _ikv.getRangeRaw(1, 11);
    expect(range.length, 11);
    var x = range.entries.first;
    expect(x.key, 'Aaron Burr');
  });

  test('Getting range works', () async {
    var range = await _ikv.getRange(1, 11);
    expect(range.length, 11);
    var x = range.entries.first;
    expect(x.key, 'Aaron Burr');
    expect(x.value,
        '<div><i>noun</i></div><div>United States politician who served as vice president under Jefferson; he mortally wounded his political rival Alexander Hamilton in a duel and fled south <i>(1756-1836)</i></div><div><span>•</span> <i>Syn</i>: ↑<a href=Burr>Burr</a></div><div><span>•</span> <i>Instance Hypernyms</i>: ↑<a href=politician>politician</a>, ↑<a href=politico>politico</a>, ↑<a href=pol>pol</a>, ↑<a href=political leader>political leader</a></div>');
  });

  test('Can get value by key', () async {
    var v = await _ikv['зараць'];
    expect(v, '<div>вспахать</div>');
  });

  test('Can get raw uncompressed value', () async {
    var v = await _ikv.valueRawCompressed('зараць');
    expect(v.isNotEmpty, true);
    v = await _ikv.valueRawCompressedAt(2);
    expect(v.isNotEmpty, true);
  });

  test('Key keysStartingWith() finds the key', () {
    var keys = _ikv.keysStartingWith('aerosol', 3);
    expect(keys[0], 'aerosol bomb');
  });

  test('Key in keysStartingWith() is trimmed', () {
    var keys = _ikv.keysStartingWith(' Acer ', 1);
    expect(keys.length, 1);
  });

  test('Non existing keys return empty result', () async {
    expect(_ikv.containsKey('wewer'), false);
    expect(await _ikv['wewer'], '');
    expect((await _ikv.valueRawCompressed('wewer')).isEmpty, true);
  });
}

void runCaseInsensitiveTests() {
  test('Out of order keys are fixed (ё isnt below я)', () {
    var k = _ikv.keys[_ikv.keys.length - 1];
    expect(k, 'яскравасьць');
  });

  test('Case-insensitive search by key works', () async {
    var v = await _ikv['afrikaans'];
    expect(
        v.startsWith(
            '<div><b>I</b></div><div><i>noun</i></div><div>an official language of the'),
        true);
  });

  test('Keys are sorted', () {
    expect(_ikv.keys[_ikv.keys.length - 8] == 'эліпс', true);
    expect(_ikv.keys[_ikv.keys.length - 3] == 'юродзівасьць', true);
    expect(_ikv.keys[_ikv.keys.length - 1] == 'яскравасьць', true);
  });

  test('Key keysStartingWith() limits the result', () {
    var keys = _ikv.keysStartingWith('an', 3);
    expect(keys.length, 3);
  });

  test('Key keysStartingWith() conducts case-insensitive search', () {
    var keys = _ikv.keysStartingWith('ЗЬ');
    expect(keys.length, 6);
  });

  test(
      'Key keysStartingWith() search cases insesitive keys but returns original keys',
      () {
    var keys = _ikv.keysStartingWith('неглижэ');
    expect(keys[0], 'негліжэ');
  });

  test('"и" and "i" subsitute wroks when looking up Belarusian words', () {
    var keys = _ikv.keysStartingWith('ихтыёл');
    expect(keys.length, 1);
  });

  test('"и" and "i" subsitute wroks when getting value by original key',
      () async {
    var keys = _ikv.keysStartingWith('ихтыёл');
    expect(keys.length, 1);
    var val = await _ikv[keys[0]];
    expect(val, '<div>ихтиол</div>');
  });

  test('Consolidated keysStartingWith works on case-insensitive keys', () {
    var m = <String, String>{'': '', 'wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
    var ik = IkvPack.fromMap(m);
    var ikvs = [_ikv, _ikv, ik];

    var keys = IkvPack.consolidatedKeysStartingWith(ikvs, 'зьнізіць');

    expect(keys.length, 1);
    expect(keys[0], 'зьнізіць');

    keys = IkvPack.consolidatedKeysStartingWith(ikvs, 'b', 10);
    expect(keys.length, 10);
  });

  test('Consolidated keysStartingWith returns original keys', () {
    var m = <String, String>{'': '', 'Wew': 'dsdsd', 'sss': '', 'sdss': 'd'};
    var ik = IkvPack.fromMap(m);
    var ikvs = [_ikv, _ikv, ik];

    var keys = IkvPack.consolidatedKeysStartingWith(ikvs, 'w');
    expect(keys.contains('Wew'), true);
  });
}
