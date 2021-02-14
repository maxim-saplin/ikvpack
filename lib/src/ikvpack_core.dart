library ikvpack_core;

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:isolate';
import 'dart:math';
import 'dart:typed_data';

import 'package:archive/archive.dart';
import 'package:ikvpack/ikvpack.dart';

import 'storage_impl/storage_vm.dart'
    if (dart.library.io) 'storage_impl/storage_vm.dart'
    if (dart.library.html) 'storage_impl/storage_web.dart';

part 'ikvpack_storage.dart';
part 'ikvpack_isolates.dart';

List<String> keysStartingWith(Iterable<IkvPack> packs) {
  var keys = <String>[];
  return keys;
}

class IkvPack {
  final Storage? _storage;

  bool _shadowKeysUsed = false;
  bool get shadowKeysUsed => _shadowKeysUsed;

  IkvPack._(String path, [this.keysCaseInsensitive = true])
      : _valuesInMemory = false,
        _storage = Storage(path);

  IkvPack.__([this.keysCaseInsensitive = true])
      : _valuesInMemory = true,
        _storage = null;

  static Future<IkvPack> load(String path, [keysCaseInsensitive = true]) async {
    var ikv = IkvPack._(path, keysCaseInsensitive);
    late IkvPackData data;

    try {
      data = await ikv._storage!.readSortedData(keysCaseInsensitive);
    } catch (e) {
      ikv._storage?.dispose();
      rethrow;
    }

    _build(ikv, data, keysCaseInsensitive);

    return ikv;
  }

  static void _build(IkvPack ikv, IkvPackData data, keysCaseInsensitive,
      [Function(int progressPercent)? updateProgress]) {
    if (updateProgress?.call(0) == true) return;
    ikv._originalKeys = data.originalKeys;

    if (updateProgress?.call(1) == true) return;

    if (data.shadowKeys.isNotEmpty) {
      if (ikv._storage == null ||
          !(ikv._storage!.noUpperCaseFlag && ikv._storage!.noOutOfOrderFlag)) {
        ikv._shadowKeys = data.shadowKeys;
        ikv._shadowKeysUsed = true;
      } else {
        ikv._shadowKeysUsed = false;
      }
    } else {
      // check if there's need for shadow keys
      if (keysCaseInsensitive &&
          (ikv._storage == null ||
              !(ikv._storage!.noUpperCaseFlag &&
                  ikv._storage!.noOutOfOrderFlag))) {
        ikv._shadowKeysUsed = true;
        ikv._shadowKeys = List.generate(ikv._originalKeys.length, (i) {
          var k = ikv._storage != null && ikv._storage!.noUpperCaseFlag
              ? ikv._originalKeys[i]
              : ikv._originalKeys[i].toLowerCase();
          if (!ikv._storage!.noOutOfOrderFlag) k = fixOutOfOrder(k);
          return k;
        }, growable: false);
      } else {
        ikv._shadowKeysUsed = false;
      }
    }

    if (updateProgress?.call(30) == true) return;

    ikv._keysReadOnly = UnmodifiableListView<String>(ikv._originalKeys);

    if (updateProgress?.call(60) == true) return;
    if (data.keyBaskets.isNotEmpty) {
      ikv._keyBaskets = data.keyBaskets;
    } else {
      ikv._buildBaskets();
    }

    if (updateProgress?.call(100) == true) return;
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
      var data = await pool.scheduleJob(IkvPooledJob(path, keysCaseInsensitive))
          as IkvPackData;
      var ikv = IkvPack._(path, keysCaseInsensitive);
      _build(ikv, data, keysCaseInsensitive);
      // ikv._originalKeys = data.originalKeys;
      // ikv._keysReadOnly = UnmodifiableListView<String>(ikv._originalKeys);
      // ikv._keyBaskets = data.keyBaskets;
      // ikv._shadowKeysUsed = data.shadowKeys.isNotEmpty;
      // ikv._shadowKeys = data.shadowKeys;
      ikv._storage?.reopenFile();

      completer.complete(ikv);
    } catch (e) {
      completer.completeError(e);
    }

    return completer.future;
  }

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
  List<String> _shadowKeys = [];
  List<KeyBasket> _keyBaskets = [];
  List<Uint8List> _values = [];

  UnmodifiableListView<String> _keysReadOnly = UnmodifiableListView<String>([]);
  UnmodifiableListView<String> get keys => _keysReadOnly;

  /// Web implementation does not support indexed keys
  //bool get indexedKeys => true;

  final bool _valuesInMemory;

  /// Default constructor reads keys into memory while access file (or Indexed DB in Web)
  /// when a value is needed. fromMap() constructor stores everythin in memory
  bool get valuesInMemory => _valuesInMemory;

  /// String values are compressed via Zlib, stored that way and are decompresed when values fetched
  /// map - the source for Keys/Values
  /// keysCaseInsensitive - lower cases shadow keys will be built to conduct fast case-insensitive searches while preserving original keys
  /// updateProgress - a callback to use to push updates of building the object, only usefull when run in isolate
  IkvPack.fromMap(Map<String, String> map,
      [this.keysCaseInsensitive = true,
      Function(int progressPercent)? updateProgress,
      bool awaitProgreess = false])
      : _valuesInMemory = true,
        _storage = null {
    var entries = _getSortedEntriesFromMap(map);
    if (updateProgress != null) updateProgress(5);

    _buildOriginalKeys(entries);
    if (updateProgress != null) updateProgress(10);

    _buildShadowKeys(entries);

    var progress = 15;
    var prevProgress = 15;

    _values = List.generate(entries.length, (i) {
      var utf = utf8.encode(entries[i].value);
      var zip = Uint8List.fromList(Deflate(utf).getBytes());
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

  static Future<IkvPack> buildFromMapInIsolate(Map<String, String> map,
      [keysCaseInsensitive = true,
      Function(int progressPercent)? updateProgress]) {
    var ic = CallbackIsolate(IkvCallbackJob(map, keysCaseInsensitive));
    return ic.run((arg) => updateProgress?.call(arg));
  }

  /// Building the object can be time consuming and can block the main UI thread, splitting the
  /// build into multiple microtasks via awaiting updateProgress claback (and giving control up the stream).
  /// If the progress callback returns null build process is canceled, the method returns null
  /// Future<bool> _awaitableUpdateProgeress(int progress) {
  ///  if (_canceled) return null;
  ///  return Future(() {
  ///    propgressProperty = progress;
  ///    notifyListener()
  ///    return false;
  ///  });
  /// }
  /// ...
  /// var ikv = await IkvPack.buildFromMapAsync(map, true, (progress) async {
  ///      return _awaitableUpdateProgeress(progress);
  ///    });
  static Future<IkvPack?> buildFromMapAsync(Map<String, String> map,
      [keysCaseInsensitive = true,
      Future? Function(int progressPercent)? updateProgress]) async {
    var ikv = IkvPack.__(keysCaseInsensitive);

    var entries = ikv._getSortedEntriesFromMap(map);

    if (updateProgress != null && await updateProgress(5) == null) return null;

    ikv._buildOriginalKeys(entries);
    if (updateProgress != null && await updateProgress(10) == null) return null;

    ikv._buildShadowKeys(entries);
    if (updateProgress != null && await updateProgress(15) == null) return null;

    var progress = 15;
    var prevProgress = 15;

    ikv._values = <Uint8List>[];

    for (var i = 0; i < entries.length; i++) {
      var utf = utf8.encode(entries[i].value);
      var zip = Uint8List.fromList(Deflate(utf).getBytes());
      if (updateProgress != null) {
        progress = (15 + 80 * i / entries.length).round();
        if (progress != prevProgress) {
          prevProgress = progress;
          if (await updateProgress(progress) == null) return null;
        }
      }

      ikv._values.add(zip);
    }

    if (updateProgress != null && await updateProgress(95) == null) return null;

    ikv._keysReadOnly = UnmodifiableListView<String>(ikv._originalKeys);
    ikv._buildBaskets();
    if (updateProgress != null && await updateProgress(100) == null) {
      return null;
    }

    return ikv;
  }

  /// Creates object from binary DIKT image (e.g. when DIKT file is loaded to memory)
  IkvPack.fromBytes(ByteData bytes,
      [this.keysCaseInsensitive = true,
      Function(int progressPercent)? updateProgress])
      : _valuesInMemory = true,
        _storage = null {
    if (updateProgress?.call(0) == true) return;
    var t = parseBinary(bytes, keysCaseInsensitive);

    if (updateProgress?.call(30) == true) return;

    _build(this, t.item1, keysCaseInsensitive,
        (progress) => updateProgress?.call((30 + 0.69 * progress).round()));

    _values = t.item2;
    if (updateProgress?.call(100) == true) return;
  }

  /// Creates object from binary DIKT image (e.g. when DIKT file is loaded to memory)
  /// Can report progress and be canceled, see buildFromMapAsync() for details
  static Future<IkvPack?> buildFromBytesAsync(ByteData bytes,
      [keysCaseInsensitive = true,
      Future? Function(int progressPercent)? updateProgress]) async {
    if (updateProgress != null && await updateProgress(0) == null) return null;
    var t = parseBinary(bytes, keysCaseInsensitive);
    if (updateProgress != null && await updateProgress(30) == null) return null;

    var ikv = IkvPack.__(keysCaseInsensitive);

    _build(ikv, t.item1, keysCaseInsensitive, (progress) async {
      return await updateProgress?.call(30 + (0.69 * progress).round());
    });

    ikv._values = t.item2;

    if (updateProgress != null && await updateProgress(100) == null) {
      return null;
    }

    return ikv;
  }

  void _buildShadowKeys(List<_Triple> entries) {
    if (keysCaseInsensitive) {
      _shadowKeysUsed = true;
      _shadowKeys = List.generate(
          entries.length, (i) => entries[i].keyLowerCase,
          growable: false);
    } else {
      _shadowKeysUsed = false;
    }
  }

  void _buildOriginalKeys(List<_Triple> entries) {
    _originalKeys =
        List.generate(entries.length, (i) => entries[i].key, growable: false);
  }

  List<_Triple<String>> _getSortedEntriesFromMap(Map<String, String> map) {
    assert(map.isNotEmpty, 'Key/Value map can\'t be empty');

    Iterable<_Triple<String>>? entries;

    if (keysCaseInsensitive) {
      entries =
          map.entries.map((e) => _Triple<String>.lowerCase(e.key, e.value));
    } else {
      entries =
          map.entries.map((e) => _Triple<String>.noLowerCase(e.key, e.value));
    }

    var list = _fixKeysAndValues<String>(entries);

    assert(list.isNotEmpty, 'Refined Key/Value collection can\'t be empty');

    if (keysCaseInsensitive) {
      list.sort((e1, e2) => e1.keyLowerCase.compareTo(e2.keyLowerCase));
    } else {
      list.sort((e1, e2) => e1.key.compareTo(e2.key));
    }

    return list;
  }

  // List<_Triple<Uint8List>> _getSortedEntriesFromLists(
  //     List<String> keys, List<Uint8List> values) {
  //   assert(keys.isNotEmpty, 'Keys can\'t be empty');
  //   assert(values.isNotEmpty, 'Values can\'t be empty');
  //   if (keys.length != values.length) {
  //     throw 'keys.length isn\'t equal to values.length';
  //   }

  //   Iterable<_Triple<Uint8List>>? entries;

  //   if (keysCaseInsensitive) {
  //     entries = List<_Triple<Uint8List>>.generate(keys.length,
  //         (index) => _Triple<Uint8List>.lowerCase(keys[index], values[index]));
  //   } else {
  //     entries = List<_Triple<Uint8List>>.generate(
  //         keys.length,
  //         (index) =>
  //             _Triple<Uint8List>.noLowerCase(keys[index], values[index]));
  //   }

  //   var list = _fixKeysAndValues<Uint8List>(entries);

  //   assert(list.isNotEmpty, 'Refined Key/Value collection can\'t be empty');

  //   if (keysCaseInsensitive) {
  //     list.sort((e1, e2) => e1.keyLowerCase.compareTo(e2.keyLowerCase));
  //   } else {
  //     list.sort((e1, e2) => e1.key.compareTo(e2.key));
  //   }

  //   return list;
  // }

  List<_Triple<T>> _fixKeysAndValues<T>(Iterable<_Triple> entries) {
    var fixed = <_Triple<T>>[];

    for (var e in entries) {
      if (e.value.isNotEmpty) {
        var s = e.key;
        s = s.replaceAll('\n', '');
        s = s.replaceAll('\r', '');

        if (s.isNotEmpty) {
          if (s.length > 255) s = s.substring(0, 255);
          fixed.add(_Triple<T>.lowerCase(s, e.value));
        }
      }
    }

    return fixed;
  }

  void _buildBaskets() {
    var list = _originalKeys;
    if (_shadowKeysUsed) list = _shadowKeys;

    var firstLetter = list[0][0].codeUnits[0];
    var index = 0;

    if (list.length == 1) {
      _keyBaskets.add(KeyBasket(firstLetter, index, index));
      return;
    }

    for (var i = 1; i < list.length; i++) {
      var newFirstLetter = list[i][0].codeUnits[0];
      if (newFirstLetter != firstLetter) {
        _keyBaskets.add(KeyBasket(firstLetter, index, i - 1));
        firstLetter = newFirstLetter;
        index = i;
      }
    }

    _keyBaskets.add(KeyBasket(firstLetter, index, list.length - 1));
  }

  /// Serialize object to a given file (on VM) or IndexedDB (in Web)
  /// updateProgress callback is only implemented for Web and ignored on VM
  /// Return 'true' from updateProgress to break the operation
  /// Unlike buildFromMapAsyn()c there's no need to return Future from the callback (and await it inside the method to free microtask queue and unblock UI, there're other awaits that allow to do this without extra Future)
  Future<void> saveTo(String path,
      [Function(int progressPercent)? updateProgress]) async {
    var data = IkvPackData(_originalKeys, _shadowKeys, _keyBaskets);
    return saveToPath(path, data, _values, updateProgress);
  }

  /// Creates a list of entries for a given range (if provided) or all keys/values.
  /// Key order is preserved
  Future<LinkedHashMap<String, Uint8List>> getRangeRaw(
      int? startIndex, int? endIndex) async {
    var start = startIndex ?? 0;
    var end = endIndex ?? length - 1;

    if (start >= length) throw 'startIndex can\'t be greater than length-1';
    if (end < 0) throw 'endIndex can\'t be negative';
    if (end <= start) throw 'endIndex must be greater than startIndex';

    // ignore: prefer_collection_literals
    var result = LinkedHashMap<String, Uint8List>();

    if (_storage != null && !_storage!.useIndexToGetValue) {
      for (var i = start; i <= end; i++) {
        result[_originalKeys[i]] = await valueRawCompressed(_originalKeys[i]);
      }
    } else {
      for (var i = start; i <= end; i++) {
        result[_originalKeys[i]] = await valueRawCompressedAt(i);
      }
    }

    return result;
  }

  /// Creates a list of entries for a given range (if provided) or all keys/values.
  /// Key order is preserved
  Future<LinkedHashMap<String, String>> getRange(
      int? startIndex, int? endIndex) async {
    var start = startIndex ?? 0;
    var end = endIndex ?? length - 1;

    if (start >= length) throw 'startIndex can\'t be greater than length-1';
    if (end < 0) throw 'endIndex can\'t be negative';
    if (end <= start) throw 'endIndex must be greater than startIndex';

    // ignore: prefer_collection_literals
    var result = LinkedHashMap<String, String>();

    if (_storage != null && !_storage!.useIndexToGetValue) {
      for (var i = start; i <= end; i++) {
        result[_originalKeys[i]] = await value(_originalKeys[i]);
      }
    } else {
      for (var i = start; i <= end; i++) {
        result[_originalKeys[i]] = await valueAt(i);
      }
    }

    return result;
  }

  int get length => _originalKeys.length;

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Future<String> valueAt(int index) async {
    var bytes = valuesInMemory
        ? Inflate(_values[index]).getBytes()
        : Inflate(_storage!.useIndexToGetValue
                ? await _storage!.valueAt(index)
                : await _storage!.value(_originalKeys[index]))
            .getBytes();

    var value = utf8.decode(bytes, allowMalformed: true);
    return value;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Future<Uint8List> valueRawCompressedAt(int index) async {
    var bytes = valuesInMemory
        ? _values[index]
        : _storage!.useIndexToGetValue
            ? await _storage!.valueAt(index)
            : await _storage!.value(_originalKeys[index]);

    return bytes;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Future<String> value(String key) async {
    var index = indexOf(key);
    if (index < 0) return ''; //throw 'key not foiund';

    return _storage != null && !_storage!.useIndexToGetValue
        ? utf8.decode(Inflate(await _storage!.value(key)).getBytes(),
            allowMalformed: true)
        : await valueAt(index);
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Future<Uint8List> valueRawCompressed(String key) async {
    var index = indexOf(key);
    if (index < 0) return Uint8List(0); //throw 'key not foiund';

    return _storage != null && !_storage!.useIndexToGetValue
        ? Uint8List.fromList(await _storage!.value(key))
        : await valueRawCompressedAt(index);
  }

  /// Returns decompressed value
  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Future<String> operator [](String key) async {
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
      key = fixOutOfOrder(key);
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

  int _narrowDownFirst(String key, KeyBasket b, List<String> list) {
    var l = b.startIndex;
    var r = b.endIndex;

    while (l <= r) {
      var m = (l + (r - l) / 2).round();

      var res = key.compareTo(list[m]);

      // Check if x is present at mid
      if (res == 0) break;

      if (res > 0) {
        // If x greater, ignore left half
        l = m + 1;
      } else {
        // If x is smaller, ignore right half
        r = m - 1;
      }
    }

    return l < r ? l : r;
  }

// Used by consolidated lookup to recover original keys
  //int _kswi = 0;

  final List<String> _kswOriginalKeys = [];

  /// Efficiently search keys that start with given srting. if keysCaseInsensitive == true
  /// than use lower case shadow keys for comparisons - in this case there can be 2+ different
  /// original keys while when kower case thay will be the same (e.g. Aa and aA are both aa when lower case, JIC original keys are always unique, shadow keys are not guaranteed)
  ///
  List<String> keysStartingWith(String key,
      [int maxResults = 100, bool returnShadowKeys = false]) {
    //var keys = <String>[];
    var list = _originalKeys;
    key = key.trim();

    if (key.isEmpty) return [];

    if (keysCaseInsensitive) {
      key = key.toLowerCase();
    }

    if (_shadowKeysUsed) {
      key = fixOutOfOrder(key);
      list = _shadowKeys;
    }

    var fl = key[0].codeUnits[0];

    //var matches = <String, int>{};

    var result = <String>[];
    _kswOriginalKeys.clear();

    for (var b in _keyBaskets) {
      if (b.firstLetter == fl) {
        var startIndex = b.startIndex;
        var endIndex = b.endIndex;

        // if (key.length == 1) {
        //   _kswi = startIndex;
        //   return (_shadowKeysUsed && returnShadowKeys
        //           ? _shadowKeys
        //           : _originalKeys)
        //       .sublist(startIndex, min(endIndex + 1, startIndex + maxResults));
        // } else {
        if (key.length > 1) {
          var f = _narrowDownFirst(key, b, list);
          startIndex = f > -1 ? f : startIndex;
        }

        var i = startIndex;
        var prevK = '';
        for (i; i <= endIndex; i++) {
          if (list[i].startsWith(key)) {
            var k = _shadowKeysUsed && returnShadowKeys
                ? _shadowKeys[i]
                : _originalKeys[i];
            if (k != prevK) {
              result.add(k);
              if (returnShadowKeys) _kswOriginalKeys.add(_originalKeys[i]);
              prevK = k;
            }
            if (result.length >= maxResults) {
              //_kswi = i - result.length + 1;
              return result;
            }
            // if (!matches.containsKey(k)) {
            //   matches[k] = 0;
            // }
            // if (matches.length >= maxResults) {
            //   _kswi = i - matches.length + 1;
            //   return matches.keys.toList();
            // }
          }
        }

        break;
      }
    }

    //return matches.keys.toList();
    return result;
  }

  void dispose() {
    _storage?.dispose();
  }

  int get sizeBytes {
    if (valuesInMemory || _storage == null) return -1;
    return _storage!.sizeBytes;
  }

  /// Get Ikv size and length without expensively loading it into memory
  static Future<IkvInfo> getInfo(String path) async {
    return storageGetInfo(path);
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
        return Tupple(e, p.keysCaseInsensitive ? p._kswOriginalKeys[i++] : e);
      }));
      if (tuples.length > maxResults) max2 = (maxResults / 2).floor();
      if (tuples.length > 3 * maxResults) max2 = (maxResults / 2).floor();
    }

    if (tuples.isNotEmpty) {
      tuples = _distinct(tuples);

      if (tuples.length > maxResults) {
        tuples = tuples.sublist(0, min(maxResults, tuples.length));
      }

      //recover original keys
      matches = tuples.map((e) => e.item2).toList();
    }

    return matches;
  }

  /// Can be slow
  Future<Stats> getStats() async {
    if (_storage == null) {
      throw 'Cant get stats on in-memory instance, only file based';
    }
    if (!_storage!.binaryStore) {
      throw 'Cant get stats on non-file based IkvPack';
    }

    var _headers = _storage!.headers;

    var keysNumber = _headers.count;
    var keysBytes = _headers.offsetsOffset - 16 - _headers.count * 2;
    //var altKeysBytes = 0;
    var valuesBytes = sizeBytes - _headers.valuesOffset;
    var keysTotalChars = _originalKeys.fold<int>(
        0, (previousValue, element) => previousValue + element.length);

    var distinctKeysNumber = 1;

    var minKeyLength = 1000000;
    var maxKeyLength = 0;

    var prev = _originalKeys[0];
    for (var i = 1; i < _originalKeys.length; i++) {
      if (_originalKeys[i].length > maxKeyLength) {
        maxKeyLength = _originalKeys[i].length;
      }
      if (_originalKeys[i].length < minKeyLength) {
        minKeyLength = _originalKeys[i].length;
      }
      if (prev != _originalKeys[i]) {
        distinctKeysNumber++;
        prev = _originalKeys[i];
      }
      // altKeysBytes += keys[i].codeUnits.fold(0, (previousValue, element) {
      //   return previousValue +
      //       (element > 127
      //           ? (element > 2047 ? (element > 65535 ? 4 : 3) : 2)
      //           : 1);
      // });
    }

    var shadowDiff = -1;

    if (shadowKeysUsed) {
      for (var i = 0; i < _originalKeys.length; i++) {
        if (_shadowKeys[i] != _originalKeys[i]) {
          shadowDiff++;
        }
      }
    }

    var stats = Stats(
        keysNumber,
        distinctKeysNumber,
        shadowDiff,
        keysBytes,
        valuesBytes,
        _storage!.sizeBytes,
        keysTotalChars,
        minKeyLength,
        maxKeyLength);

    return stats;
  }

  static Future<String> getStatsAsCsv(Iterable<IkvPack> packs) async {
    var s = StringBuffer(
        'Ikv;keysNumber;distinctKeysNumber;shadowKeysDifferentFromOrigNumber;'
        'keysBytes;valuesBytes;keysTotalChars;minKeyLength;maxKeyLength;avgKeyLength;'
        'avgKeyBytes;avgCharBytes;avgValueBytes;');

    for (var i in packs) {
      print('Getting stats for ${i._storage!.path}');
      var stats = await i.getStats();
      s.writeln();
      s.write(i._storage!.path);
      s.write(';');
      s.write(stats.keysNumber);
      s.write(';');
      s.write(stats.distinctKeysNumber);
      s.write(';');
      s.write(stats.shadowKeysDifferentFromOrigNumber);
      s.write(';');
      s.write(stats.keysBytes);
      s.write(';');
      s.write(stats.valuesBytes);
      s.write(';');
      s.write(stats.keysTotalChars);
      s.write(';');
      s.write(stats.minKeyLength);
      s.write(';');
      s.write(stats.maxKeyLength);
      s.write(';');
      s.write(stats.avgKeyLength);
      s.write(';');
      s.write(stats.avgKeyBytes);
      s.write(';');
      s.write(stats.avgCharBytes);
      s.write(';');
      s.write(stats.avgValueBytes);
      s.write(';');
    }

    return s.toString();
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

class KeyBasket {
  final int firstLetter;
  final int startIndex;
  int get endIndex => _endIndex;
  // _endIndex is set after object create while reading baskets from file which stores on startIndex and endindex is calculated as difference of startIndex of adjacent records
  // ignore: prefer_final_fields
  int _endIndex;

  KeyBasket(this.firstLetter, this.startIndex, this._endIndex);

  String get firstLetterString => String.fromCharCode(firstLetter);
  int get length => _endIndex - startIndex;
}

class _Triple<T> {
  final String key;
  final String keyLowerCase;
  final T value;

  _Triple.lowerCase(this.key, this.value)
      : keyLowerCase = fixOutOfOrder(key.toLowerCase());

  _Triple.noLowerCase(this.key, this.value) : keyLowerCase = '';
}

final _c1 = 'ё'.codeUnits[0];
final _cc1 = 'е'.codeUnits[0];
final _c2 = 'і'.codeUnits[0];
final _cc2 = 'и'.codeUnits[0];
final _c3 = 'ў'.codeUnits[0];
final _cc3 = 'у'.codeUnits[0];

/// Out of order Cyrylic chars
/// я - 1103
///
/// е - 1077 (й 1078)
/// ё - 1105  <---
///
/// з - 1079 (и 1080)
/// і - 1110  <---
///
/// у - 1091
/// ў - 1118  <---
/// ѝ/1117, ќ/1116, ћ/1115, њ/1114, љ/1113, ј/1112, ї/1111, і/1110, ѕ/1109,
/// є/1108, ѓ/1107, ђ/1106, ё/1105, ѐ/1104
/// http://www.ltg.ed.ac.uk/~richard/utf-8.cgi?input=1103&mode=decimal

/// Belarusian alphabet
/// АаБбВвГгДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуЎўФфХхЦцЧчШшЫыЬьЭэЮюЯя
/// АБВГДЕЁЖЗІЙКЛМНОПРСТУЎФХЦЧШЫЬЭЮЯ
/// абвгдеёжзійклмнопрстуўфхцчшыьэюя
@pragma('vm:prefer-inline')
@pragma('dart2js:tryInline')
String fixOutOfOrder(String value) {
  //value = value.replaceAll('ё', 'е').replaceAll('і', 'и').replaceAll('ў', 'у');

  var cus = Uint16List.fromList(value.codeUnits);
  var changed = false;

  for (var i = 0; i < value.length; i++) {
    if (cus[i] == _c1) {
      cus[i] = _cc1;
      changed = true;
    } else if (cus[i] == _c2) {
      cus[i] = _cc2;
      changed = true;
    } else if (cus[i] == _c3) {
      cus[i] = _cc3;
      changed = true;
    }
  }
  if (changed) {
    return String.fromCharCodes(cus);
  }

  return value;
}
