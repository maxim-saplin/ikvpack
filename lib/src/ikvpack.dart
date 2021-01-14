import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';
import 'dart:typed_data';
import 'package:ikvpack/ikvpack.dart';

import 'storage_vm.dart'
    if (dart.library.io) 'storage_vm.dart'
    if (dart.library.html) 'storage_web.dart';

List<String> keysStartingWith(Iterable<IkvPack> packs) {
  var keys = <String>[];
  return keys;
}

class IkvPack {
  final Storage? _storage;

  bool _shadowKeysUsed = false;
  bool get shadowKeysUsed => _shadowKeysUsed;

  IkvPack(String path, [this.keysCaseInsensitive = true])
      : _valuesInMemory = false,
        _storage = Storage(path) {
    try {
      _originalKeys = _storage!.readSortedKeys();
    } catch (e) {
      _storage?.dispose();
      rethrow;
    }

    // check if there's need for shadow keys
    if (!(_storage!.noUpperCaseFlag && _storage!.noOutOfOrderFlag)) {
      if (keysCaseInsensitive) {
        _shadowKeysUsed = true;
        _shadowKeys = List.generate(_originalKeys.length, (i) {
          var k = _storage!.noUpperCaseFlag
              ? _originalKeys[i]
              : _originalKeys[i].toLowerCase();
          if (!_storage!.noOutOfOrderFlag) k = _fixOutOfOrder(k);
          return k;
        }, growable: false);
      }
    } else {
      _shadowKeysUsed = false;
    }

    _keysReadOnly = UnmodifiableListView<String>(_originalKeys);

    _buildBaskets();
  }

  static Future<IkvPack> loadInIsolate(String path,
      [keysCaseInsensitive = true]) async {
    var completer = Completer<IkvPack>();
    var receivePort = ReceivePort();
    var errorPort = ReceivePort();
    var params = _IsolateParams(
        receivePort.sendPort, errorPort.sendPort, path, keysCaseInsensitive);

    var isolate = await Isolate.spawn<_IsolateParams>(_loadIkv, params,
        errorsAreFatal: true);

    receivePort.listen((data) {
      var ikv = (data as IkvPack);
      isolate.kill();
      ikv._storage?.reopenFile();
      completer.complete(ikv);
    });

    errorPort.listen((e) {
      completer.completeError(e);
    });

    return completer.future;
  }

  /// !Warning! Isolate pool needs to be maually started before using this method
  /// and stoped when not needed anymore
  static Future<IkvPack> loadInIsolatePool(IsolatePool pool, String path,
      [keysCaseInsensitive = true]) async {
    var completer = Completer<IkvPack>();

    try {
      var ikv = await pool.scheduleJob(IkvPooledJob(path, keysCaseInsensitive));
      ikv._storage?.reopenFile();
      completer.complete(ikv);
    } catch (e) {
      completer.completeError(e);
    }

    return completer.future;
  }

  static Future<IkvPack> buildFromMapInIsolate(Map<String, String> map,
      [keysCaseInsensitive = true,
      Function(int progressPercent)? updateProgress]) {
    var ic = CallbackIsolate(IkvCallbackJob(map, keysCaseInsensitive));
    return ic.run((arg) => updateProgress?.call(arg));
  }

  //TODO, add test
  /// Deletes file on disk or related IndexedDB in Web
  static void delete(String path) {
    deleteFromPath(path);
  }

  /// Do not do strcit comparisons by ignoring case.
  /// Make lowercase shadow version of keys and uses those for lookups.
  /// Also fix out of-order-chars, e.g. replace cyrylic 'ё' (code 1110 which in alphabet stands before 'я', code 1103)
  /// with 'е' (code 1077)
  bool keysCaseInsensitive = true;

  List<String> _originalKeys = [];
  // TODO, Test performance/mem consumption, maybe make 5-10 char lower case keys index for fast searches, not full keys
  List<String> _shadowKeys = [];
  final List<_KeyBasket> _keyBaskets = [];
  List<List<int>> _values = [];

  // Archive packaghe appeared to be faster in compressing and decompressing
  // though caught strange outofmemory exception on version 3.0-nullsafe
  // Decided to used standrad Dart's codec
  //final ZLibDecoder decoder = ZLibDecoder();

  final ZLibCodec codec = ZLibCodec(
      level: 7,
      memLevel: ZLibOption.maxMemLevel,
      raw: true,
      strategy: ZLibOption.strategyDefault);

  UnmodifiableListView<String> _keysReadOnly = UnmodifiableListView<String>([]);
  UnmodifiableListView<String> get keys => _keysReadOnly;

  /// Web implementation does not support indexed keys
  //bool get indexedKeys => true;

  final bool _valuesInMemory;

  /// Default constructor reads keys into memory while access file (or Indexed DB in Web)
  /// when a value is needed. fromMap() constructor stores everythin in memory
  bool get valuesInMemory => _valuesInMemory;

  /// String values are compressed via Zlib
  IkvPack.fromMap(Map<String, String> map,
      [this.keysCaseInsensitive = true,
      Function(int progressPercent)? updateProgress])
      : _valuesInMemory = true,
        _storage = null {
    var entries = _getSortedEntries(map);
    if (updateProgress != null) updateProgress(5);
    //var enc = ZLibEncoder();

    _originalKeys =
        List.generate(entries.length, (i) => entries[i].key, growable: false);
    if (updateProgress != null) updateProgress(10);

    if (keysCaseInsensitive) {
      _shadowKeysUsed = true;
      _shadowKeys = List.generate(
          entries.length, (i) => entries[i].keyLowerCase,
          growable: false);
    } else {
      _shadowKeysUsed = false;
    }

    if (updateProgress != null) updateProgress(15);

    // var utfLength = 0;
    // var zipLength = 0;

    var progress = 15;
    var prevProgress = 15;

    _values = List.generate(entries.length, (i) {
      var utf = utf8.encode(entries[i].value);
      var zip = codec.encoder.convert(utf);
      if (updateProgress != null) {
        progress = (15 + 80 * i / entries.length).round();
        if (progress != prevProgress) {
          prevProgress = progress;
          updateProgress(progress);
        }
      }

      return zip;
    }, growable: false);

    if (updateProgress != null) updateProgress(95);

    _keysReadOnly = UnmodifiableListView<String>(_originalKeys);
    _buildBaskets();
    if (updateProgress != null) updateProgress(100);
  }

  List<_Triple> _getSortedEntries(Map<String, String> map) {
    assert(map.isNotEmpty, 'Key/Value map can\'t be empty');

    Iterable<_Triple>? entries;

    if (keysCaseInsensitive) {
      entries = map.entries.map((e) => _Triple(e.key, e.value));
    } else {
      entries = map.entries.map((e) => _Triple.noLowerCase(e.key, e.value));
    }

    var list = _fixKeysAndValues(entries);

    assert(list.isNotEmpty, 'Refined Key/Value collection can\'t be empty');

    if (keysCaseInsensitive) {
      list.sort((e1, e2) => e1.keyLowerCase.compareTo(e2.keyLowerCase));
    } else {
      list.sort((e1, e2) => e1.key.compareTo(e2.key));
    }

    return list;
  }

  List<_Triple> _fixKeysAndValues(Iterable<_Triple> entries) {
    var fixed = <_Triple>[];

    for (var e in entries) {
      if (e.value.isNotEmpty) {
        var s = e.key;
        s = s.replaceAll('\n', '');
        s = s.replaceAll('\r', '');

        if (s.isNotEmpty) {
          if (s.length > 255) s = s.substring(0, 255);
          fixed.add(_Triple(s, e.value));
        }
      }
    }

    return fixed;
  }

  void _buildBaskets() {
    var list = _originalKeys;
    if (_shadowKeysUsed) list = _shadowKeys;

    var firstLetter = list[0][0];
    var index = 0;

    if (list.length == 1) {
      _keyBaskets.add(_KeyBasket(firstLetter.codeUnits[0], index, index));
      return;
    }

    for (var i = 1; i < list.length; i++) {
      var newFirstLetter = list[i][0];
      if (newFirstLetter != firstLetter) {
        _keyBaskets.add(_KeyBasket(firstLetter.codeUnits[0], index, i - 1));
        firstLetter = newFirstLetter;
        index = i;
      }
    }

    _keyBaskets
        .add(_KeyBasket(firstLetter.codeUnits[0], index, list.length - 1));
  }

  /// Serilized object to given file on VM and IndexedDB in Web
  void saveTo(String path) {
    saveToPath(path, _originalKeys, _values);
  }

  int get length => _originalKeys.length;

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  String valueAt(int index) {
    var bytes = valuesInMemory
        ? codec.decoder.convert(_values[index])
        : codec.decoder.convert(_storage!.valueAt(index));
    var value = utf8.decode(bytes, allowMalformed: true);
    return value;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Uint8List valueRawCompressedAt(int index) {
    var value = valuesInMemory ? _values[index] : _storage!.valueAt(index);

    return Uint8List.fromList(value);
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  String value(String key) {
    var index = indexOf(key);
    if (index < 0) return ''; //throw 'key not foiund';

    return _storage != null && !_storage!.useIndexToGetValue
        ? utf8.decode(codec.decoder.convert(_storage!.value(key)),
            allowMalformed: true)
        : valueAt(index);
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Uint8List valueRawCompressed(String key) {
    var index = indexOf(key);
    if (index < 0) return Uint8List(0); //throw 'key not foiund';

    return _storage != null && !_storage!.useIndexToGetValue
        ? Uint8List.fromList(_storage!.value(key))
        : valueRawCompressedAt(index);
  }

  /// Returns decompressed value
  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  String operator [](String key) {
    return value(key);
  }

  /// -1 if not found
  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  int indexOf(String key) {
    var list = _originalKeys;
    if (keysCaseInsensitive) {
      key = key.toLowerCase();
    }
    if (_shadowKeysUsed) {
      key = _fixOutOfOrder(key);
      list = _shadowKeys;
    }

    var fl = key[0].codeUnits[0];

    //var it = 0;

    for (var b in _keyBaskets) {
      if (b.firstLetter == fl) {
        var l = b.startIndex;
        var r = b.endIndex;
        while (l <= r) {
          //it++;
          var m = (l + (r - l) / 2).round();

          var res = key.compareTo(list[m]);

          // Check if x is present at mid
          if (res == 0) {
            //print('  --it ${it}');
            return m;
          }

          if (res > 0) {
            // If x greater, ignore left half
            l = m + 1;
          } else {
            // If x is smaller, ignore right half
            r = m - 1;
          }
        }
      }
    }

    //print('  --it ${it}');
    return -1;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  bool containsKey(String key) => indexOf(key) > -1;

  // int _narrowDownFirst(String key, _KeyBasket b, List<String> list) {
  //   var l = b.startIndex;
  //   var r = b.endIndex;

  //   while (l <= r) {
  //     var m = (l + (r - l) / 2).round();

  //     var res = key.compareTo(list[m]);

  //     // Check if x is present at mid
  //     if (res == 0) break;

  //     if (res > 0) {
  //       // If x greater, ignore left half
  //       l = m + 1;
  //     } else {
  //       // If x is smaller, ignore right half
  //       r = m - 1;
  //     }
  //   }

  //   return l < r ? l : r;
  // }

// Used by consolidated lookup to recover original keys
  int _kswi = 0;

  List<String> keysStartingWith(String key,
      [int maxResults = 100, bool returnShadowKeys = false]) {
    var keys = <String>[];
    var list = _originalKeys;
    key = key.trim();

    if (key.isEmpty) return keys;

    if (keysCaseInsensitive) {
      key = key.toLowerCase();
    }

    if (_shadowKeysUsed) {
      key = _fixOutOfOrder(key);
      list = _shadowKeys;
    }

    var fl = key[0].codeUnits[0];
    _kswi = -1;

    for (var b in _keyBaskets) {
      if (b.firstLetter == fl) {
        var startIndex = b.startIndex;
        var endIndex = b.endIndex;

        if (key.length == 1) {
          _kswi = startIndex;
          return (_shadowKeysUsed && returnShadowKeys
                  ? _shadowKeys
                  : _originalKeys)
              .sublist(startIndex, min(endIndex + 1, startIndex + maxResults));
        } else {
          //startIndex = _narrowDownFirst(key, b, list);

          var i = startIndex;
          for (i; i <= endIndex; i++) {
            if (list[i].startsWith(key)) {
              if (_kswi == -1) _kswi = i;
              keys.add(_shadowKeysUsed && returnShadowKeys
                  ? _shadowKeys[i]
                  : _originalKeys[i]);
              if (keys.length >= maxResults) {
                _kswi = i - keys.length + 1;
                return keys;
              }
            }
          }
        }

        break;
      }
    }

    return keys;
  }

  void dispose() {
    _storage?.dispose();
  }

  int get sizeBytes {
    if (valuesInMemory || _storage == null) return -1;
    return _storage!.sizeBytes;
  }

  bool get noOutOfOrderFlag =>
      _storage != null ? _storage!.noOutOfOrderFlag : false;
  bool get noUpperCaseFlag =>
      _storage != null ? _storage!.noUpperCaseFlag : false;

  /// Helper methods that searches for keys in a number of packs
  /// and returns a unique set of keys. If keysCaseInsesitive, shadow
  /// versions are used for matches, but original keys are returned
  static List<String> consolidatedKeysStartingWith(
      Iterable<IkvPack> packs, String value,
      [int maxResults = 100]) {
    // var sw = Stopwatch();
    // sw.start();
    var matches = <String>[];

    var max2 = maxResults;
    var tuples = <Tupple<String, String>>[];

    for (var p in packs) {
      var i = 0;
      tuples.addAll(
          p.keysStartingWith(value, max2, p.keysCaseInsensitive).map((e) {
        return Tupple(e, p._originalKeys[p._kswi + i++]);
      }));
      if (tuples.length > maxResults) max2 = (maxResults / 2).floor();
      if (tuples.length > 3 * maxResults) max2 = (maxResults / 2).floor();
    }

    // print(
    //     '|Lookup matches ${tuples.length}, ${sw.elapsedMicroseconds} microseconds');
    // sw.reset();

    if (tuples.length > 1) {
      tuples = _distinct(tuples);
      // print(
      //     '|Distinct ${tuples.length}, ${sw.elapsedMicroseconds} microseconds');
      // sw.reset();

      if (tuples.length > maxResults) {
        tuples = tuples.sublist(0, min(maxResults, tuples.length));
      }

      // print(
      //     '|Sublist ${matches.length}, ${sw.elapsedMicroseconds} microseconds');
      // sw.reset();

      //recover original keys
      matches = tuples.map((e) => e.item2).toList();

      // print('|Originals recovered, ${sw.elapsedMicroseconds} microseconds');
      // sw.reset();
    }

    // 82 dics, я being moved by Я
    // matches = distinct(matches);
    // print(
    //     '|Distinct2 ${matches.length}, ${sw.elapsedMicroseconds} microseconds');

    return matches;
  }
}

class Tupple<T1, T2> {
  final T1 item1;
  final T2 item2;

  Tupple(this.item1, this.item2);
}

List<Tupple<String, String>> _distinct(List<Tupple<String, String>> list) {
  if (list.isEmpty) return list;
  list.sort((a, b) => a.item1.compareTo(b.item1));
  var unique = <Tupple<String, String>>[];
  unique.add(list[0]);

  for (var i = 0; i < list.length; i++) {
    if (list[i].item1 != unique.last.item1) {
      unique.add(list[i]);
    }
  }

  return unique;
}

abstract class StorageBase {
  StorageBase(String path);

  bool get noOutOfOrderFlag;
  bool get noUpperCaseFlag;

  List<String> readSortedKeys();
  List<int> value(String key);
  List<int> valueAt(int index);
  int get sizeBytes;
  void dispose();
  bool get useIndexToGetValue;
  // the bellow 2 methods are workarounds for passing Storage across isolates,
  // since intenal object RandomAccessFile can't cross isolates boundaries
  // closing file is done in spawned isolate and reopening is done in main isolate
  void closeFile();
  void reopenFile();
}

class _KeyBasket {
  final int firstLetter;
  final int startIndex;
  final int endIndex;

  _KeyBasket(this.firstLetter, this.startIndex, this.endIndex);
}

class _Triple {
  final String key;
  final String keyLowerCase;
  final String value;

  _Triple(this.key, this.value)
      : keyLowerCase = _fixOutOfOrder(key.toLowerCase());

  _Triple.noLowerCase(this.key, this.value) : keyLowerCase = '';
}

// Out of order Cyrylic chars
// я - 1103
//
// е - 1077 (й 1078)
// ё - 1105  <---
//
// з - 1079 (и 1080)
// і - 1110  <---
//
// у - 1091
// ў - 1118  <---
// ѝ/1117, ќ/1116, ћ/1115, њ/1114, љ/1113, ј/1112, ї/1111, і/1110, ѕ/1109,
// є/1108, ѓ/1107, ђ/1106, ё/1105, ѐ/1104
// http://www.ltg.ed.ac.uk/~richard/utf-8.cgi?input=1103&mode=decimal

// Belarusian alphabet
// АаБбВвГгДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуЎўФфХхЦцЧчШшЫыЬьЭэЮюЯя
// АБВГДЕЁЖЗІЙКЛМНОПРСТУЎФХЦЧШЫЬЭЮЯ
// абвгдеёжзійклмнопрстуўфхцчшыьэюя
@pragma('vm:prefer-inline')
@pragma('dart2js:tryInline')
String _fixOutOfOrder(String value) {
  value = value.replaceAll('ё', 'е').replaceAll('і', 'и').replaceAll('ў', 'у');
  return value;
}

class _IsolateParams {
  final SendPort sendPort;
  final SendPort errorPort;
  final String path;
  final bool keysCaseInsensitive;

  _IsolateParams(
      this.sendPort, this.errorPort, this.path, this.keysCaseInsensitive);
}

void _loadIkv(_IsolateParams params) async {
  try {
    var ikv = IkvPack(params.path, params.keysCaseInsensitive);
    ikv._storage?.closeFile();
    params.sendPort.send(ikv);
  } catch (e) {
    params.errorPort.send(e);
  }
}

class IkvPooledJob extends PooledJob<IkvPack> {
  final String path;
  final bool keysCaseInsensitive;

  IkvPooledJob(this.path, this.keysCaseInsensitive);

  @override
  IkvPack job() {
    var ikv = IkvPack(path, keysCaseInsensitive);
    ikv._storage?.closeFile();
    return ikv;
  }
}

class IkvCallbackJob extends CallbackIsolateJob<IkvPack, int> {
  final Map<String, String> map;
  final bool keysCaseInsensitive;

  IkvCallbackJob(this.map, this.keysCaseInsensitive) : super(true);

  @override
  Future<IkvPack> jobAsync() {
    throw UnimplementedError();
  }

  @override
  IkvPack jobSync() {
    return IkvPack.fromMap(map, keysCaseInsensitive,
        (int progress) => sendDataToCallback(progress));
  }
}
