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

/// IkvPack provides API to work with string Key/Value pairs, serilizing/deserilizing
/// the data using files (dartVM) and IndexedDB(Web).
/// There're currently 2 implementations: IkvPackImpl and IkvPackProxy.
/// Factory methods within IkvPack allow creating both variants.
/// IkvPackImpl contains the actual logic behind IkvPack APIs and work with both Web and Native apps.

abstract class IkvPack {
  UnmodifiableListView<String> get keys;
  UnmodifiableListView<String> get shadowKeys;
  UnmodifiableListView<KeyBasket> get keyBaskets;

  StorageBase? get storage;
  bool get keysCaseInsensitive;
  bool get shadowKeysUsed;

  /// Default constructor reads keys into memory while access file (or Indexed DB in Web)
  /// when a value is needed. fromMap() constructor stores everythin in memory
  bool get valuesInMemory;

  bool get noOutOfOrderFlag;
  bool get noUpperCaseFlag;

  int get length;
  int get sizeBytes;

  Future<void> saveTo(String path,
      [Function(int progressPercent)? updateProgress]);

  Future<LinkedHashMap<String, Uint8List>> getRangeRaw(
      int? startIndex, int? endIndex);

  Future<LinkedHashMap<String, String>> getRange(
      int? startIndex, int? endIndex);

  Future<String> getValueAt(int index);

  Future<Uint8List> getValueRawCompressedAt(int index);

  Future<Uint8List> getValueRawCompressed(String key);

  Future<String> getValue(String key);

  Future<String> operator [](String key);

  bool containsKey(String key);

  int indexOf(String key);

  /// Efficiently search keys that start with given srting. if keysCaseInsensitive == true
  /// than use lower case shadow keys for comparisons - in this case there can be 2+ different
  /// original keys while when kower case thay will be the same (e.g. Aa and aA are both aa when lower case, JIC original keys are always unique, shadow keys are not guaranteed)
  /// Pairs are returned, the first value is the original key, the second one - shadow key
  /// IkvPackProxy (created via loadInIsolatePoolAndUseProxy) uses IsolatePool to create IkvPackImpl in separate isolates and
  /// communicates with them across isolate boundaries via sort of RPC.
  Future<List<KeyPair>> keysStartingWith(String key, [int maxResults = 100]);

  void dispose();

  Future<Stats> getStats();

  /// Deletes file on disk or related IndexedDB in Web
  static void delete(String path) {
    deleteFromPath(path);
  }

  /// Helper methods that searches for keys in a number of packs
  /// and returns a unique set of keys. If keysCaseInsesitive, shadow
  /// versions are used for matches, but original keys are returned
  static Future<List<String>> consolidatedKeysStartingWith(
      Iterable<IkvPack> packs, String value,
      [int maxResults = 100]) async {
    // var sw = Stopwatch();
    // sw.start();
    var matches = <String>[];

    var max2 = maxResults;
    var pairs = <KeyPair>[];

    var i = 0;
    const step = 12;
    var futures = <Future<List<KeyPair>>>[];

    for (var p in packs) {
      futures.add(p.keysStartingWith(value, max2));
      i++;
      if (i >= step) {
        i = 0;
        var pp = await Future.wait<List<KeyPair>>(futures);
        futures.clear();
        for (var p in pp) {
          pairs.addAll(p);
        }
        if (pairs.length > maxResults) max2 = (maxResults / 2).floor();
        if (pairs.length > 3 * maxResults) max2 = (maxResults / 2).floor();
      }
    }

    var pp = await Future.wait<List<KeyPair>>(futures);
    futures.clear();
    for (var p in pp) {
      pairs.addAll(p);
    }

    // Considerations on key consolidation
    // E.g. 'lucid' is looked up in 3 dictionaires (A, B and C) using shadow keys
    // A has LUCID and lucid, A contains lucid, C has Lucid
    // The current implementation will take whatever first original key there is
    // and return. The problem with such approach is say LUCID is returned from
    // the search and letter LUCID is used to get values from A, while a user
    // is interested in both LUCIS and lucid, only one value will be returned

    if (pairs.isNotEmpty) {
      pairs = _distinctShadow(pairs);

      if (pairs.length > maxResults) {
        pairs = pairs.sublist(0, min(maxResults, pairs.length));
      }

      //recover original keys
      matches = pairs.map((e) => e.original).toList();
    }

    return matches;
  }

  static Future<String> getStatsAsCsv(Iterable<IkvPack> packs) async {
    var s = StringBuffer(
        'Ikv;keysNumber;distinctKeysNumber;shadowKeysDifferentFromOrigNumber;'
        'keysBytes;valuesBytes;keysTotalChars;minKeyLength;maxKeyLength;avgKeyLength;'
        'avgKeyBytes;avgCharBytes;avgValueBytes;');

    for (var i in packs) {
      print('Getting stats for ${i.storage!.path}');
      var stats = await i.getStats();
      s.writeln();
      s.write(i.storage!.path);
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

  /// Get Ikv size and length without expensively loading it into memory
  static Future<IkvInfo> getInfo(String path) async {
    return storageGetInfo(path);
  }

  factory IkvPack.fromMap(Map<String, String> map,
      [keysCaseInsensitive = true,
      Function(int progressPercent)? updateProgress,
      bool awaitProgreess = false]) {
    return IkvPackImpl.fromMap(
        map, keysCaseInsensitive, updateProgress, awaitProgreess);
  }

  factory IkvPack.fromBytes(ByteData bytes,
      [keysCaseInsensitive = true,
      Function(int progressPercent)? updateProgress]) {
    return IkvPackImpl.fromBytes(bytes, keysCaseInsensitive, updateProgress);
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
    var ikv = IkvPackImpl.__(keysCaseInsensitive);

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
  /// Can report progress and be canceled, see buildFromMapAsync() for details
  static Future<IkvPack?> buildFromBytesAsync(ByteData bytes,
      [keysCaseInsensitive = true,
      Future? Function(int progressPercent)? updateProgress]) async {
    if (updateProgress != null && await updateProgress(0) == null) return null;
    var t = parseBinary(bytes, keysCaseInsensitive);
    if (updateProgress != null && await updateProgress(30) == null) return null;

    var ikv = IkvPackImpl.__(keysCaseInsensitive);

    IkvPackImpl._build(ikv, t.item1, keysCaseInsensitive, (progress) async {
      return await updateProgress?.call(30 + (0.69 * progress).round());
    });

    ikv._values = t.item2;

    if (updateProgress != null && await updateProgress(100) == null) {
      return null;
    }

    return ikv;
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
      ikv.storage?.reopenFile();
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
      var ikv = IkvPackImpl._(path, keysCaseInsensitive);
      IkvPackImpl._build(ikv, data, keysCaseInsensitive);
      ikv._storage?.reopenFile();

      completer.complete(ikv);
    } catch (e) {
      completer.completeError(e);
    }

    return completer.future;
  }

  static Future<IkvPack> loadInIsolatePoolAndUseProxy(
      IsolatePool pool, String path,
      [keysCaseInsensitive = true]) {
    return IkvPackProxy.loadInIsolatePoolAndUseProxy(
        pool, path, keysCaseInsensitive);
  }

  static Future<IkvPack> load(String path, [keysCaseInsensitive = true]) {
    return IkvPackImpl.load(path, keysCaseInsensitive);
  }
}

class IkvPackImpl implements IkvPack {
  @override
  Storage? get storage => _storage;
  final Storage? _storage;

  bool _shadowKeysUsed = false;
  @override
  bool get shadowKeysUsed => _shadowKeysUsed;

  @override
  bool get valuesInMemory => _valuesInMemory;
  final bool _valuesInMemory;

  @override
  bool get noOutOfOrderFlag =>
      storage != null ? storage!.noOutOfOrderFlag : false;
  @override
  bool get noUpperCaseFlag =>
      storage != null ? storage!.noUpperCaseFlag : false;

  @override
  int get length => _originalKeys.length;

  IkvPackImpl._(String path, [this._keysCaseInsensitive = true])
      : _valuesInMemory = false,
        _storage = Storage(path);

  IkvPackImpl.__([this._keysCaseInsensitive = true])
      : _valuesInMemory = true,
        _storage = null;

  static Future<IkvPackImpl> load(String path,
      [keysCaseInsensitive = true]) async {
    var ikv = IkvPackImpl._(path, keysCaseInsensitive);
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

  static void _build(IkvPackImpl ikv, IkvPackData data, keysCaseInsensitive,
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

  /// Do not do strcit comparisons by ignoring case.
  /// Make lowercase shadow version of keys and uses those for lookups.
  /// Also fix out of-order-chars, e.g. replace cyrylic 'ё' (code 1110 which in alphabet stands before 'я', code 1103)
  /// with 'е' (code 1077)
  @override
  bool get keysCaseInsensitive => _keysCaseInsensitive;
  final bool _keysCaseInsensitive;

  List<String> _originalKeys = [];
  List<String> _shadowKeys = [];
  List<KeyBasket> _keyBaskets = [];
  List<Uint8List> _values = [];

  UnmodifiableListView<String> _keysReadOnly = UnmodifiableListView<String>([]);
  @override
  UnmodifiableListView<String> get keys => _keysReadOnly;

  @override
  UnmodifiableListView<String> get shadowKeys =>
      UnmodifiableListView<String>(_shadowKeys);
  @override
  UnmodifiableListView<KeyBasket> get keyBaskets =>
      UnmodifiableListView<KeyBasket>(_keyBaskets);

  /// String values are compressed via Zlib, stored that way and are decompresed when values fetched
  /// map - the source for Keys/Values
  /// keysCaseInsensitive - lower cases shadow keys will be built to conduct fast case-insensitive searches while preserving original keys
  /// updateProgress - a callback to use to push updates of building the object, only usefull when run in isolate
  IkvPackImpl.fromMap(Map<String, String> map,
      [this._keysCaseInsensitive = true,
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

  /// Creates object from binary DIKT image (e.g. when DIKT file is loaded to memory)
  IkvPackImpl.fromBytes(ByteData bytes,
      [this._keysCaseInsensitive = true,
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
  @override
  Future<void> saveTo(String path,
      [Function(int progressPercent)? updateProgress]) async {
    var data = IkvPackData(_originalKeys, _shadowKeys, _keyBaskets);
    return saveToPath(path, data, _values, updateProgress);
  }

  /// Creates a list of entries for a given range (if provided) or all keys/values.
  /// Key order is preserved
  @override
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
        result[_originalKeys[i]] =
            await getValueRawCompressed(_originalKeys[i]);
      }
    } else {
      for (var i = start; i <= end; i++) {
        result[_originalKeys[i]] = await getValueRawCompressedAt(i);
      }
    }

    return result;
  }

  /// Creates a list of entries for a given range (if provided) or all keys/values.
  /// Key order is preserved
  @override
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
        result[_originalKeys[i]] = await getValue(_originalKeys[i]);
      }
    } else {
      for (var i = start; i <= end; i++) {
        result[_originalKeys[i]] = await getValueAt(i);
      }
    }

    return result;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  @override
  Future<String> getValueAt(int index) async {
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
  @override
  Future<Uint8List> getValueRawCompressedAt(int index) async {
    var bytes = valuesInMemory
        ? _values[index]
        : _storage!.useIndexToGetValue
            ? await _storage!.valueAt(index)
            : await _storage!.value(_originalKeys[index]);

    return bytes;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  @override
  Future<String> getValue(String key) async {
    var index = indexOf(key);
    if (index < 0) return ''; //throw 'key not foiund';

    return _storage != null && !_storage!.useIndexToGetValue
        ? utf8.decode(Inflate(await _storage!.value(key)).getBytes(),
            allowMalformed: true)
        : await getValueAt(index);
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  @override
  Future<Uint8List> getValueRawCompressed(String key) async {
    var index = indexOf(key);
    if (index < 0) return Uint8List(0); //throw 'key not foiund';

    return _storage != null && !_storage!.useIndexToGetValue
        ? Uint8List.fromList(await _storage!.value(key))
        : await getValueRawCompressedAt(index);
  }

  /// Returns decompressed value
  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  @override
  Future<String> operator [](String key) async {
    return getValue(key);
  }

  /// -1 if not found
  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  @override
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

    for (var b in _keyBaskets) {
      if (b.firstLetter == fl) {
        var l = b.startIndex;
        var r = b.endIndex;
        while (l <= r) {
          var m = (l + (r - l) / 2).round();

          var res = key.compareTo(list[m]);

          // Check if x is present at mid
          if (res == 0) {
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
  @override
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

  @override
  Future<List<KeyPair>> keysStartingWith(String key,
      [int maxResults = 100]) async {
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

    var result = <KeyPair>[];

    for (var b in _keyBaskets) {
      if (b.firstLetter == fl) {
        var startIndex = b.startIndex;
        var endIndex = b.endIndex;

        if (key.length > 1) {
          var f = _narrowDownFirst(key, b, list);
          startIndex = f > -1 ? f : startIndex;
        }

        var i = startIndex;
        var prevK = '';
        for (i; i <= endIndex; i++) {
          if (list[i].startsWith(key)) {
            var k = _originalKeys[i];
            if (k != prevK) {
              result.add(KeyPair(k, shadowKeysUsed ? _shadowKeys[i] : ''));

              prevK = k;
            }
            if (result.length >= maxResults) {
              return result;
            }
          }
        }

        break;
      }
    }

    return result;
  }

  @override
  void dispose() {
    _storage?.dispose();
  }

  @override
  int get sizeBytes {
    if (valuesInMemory || _storage == null) return -1;
    return _storage!.sizeBytes;
  }

  /// Can be slow
  @override
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
}

class Tuple<T1, T2> {
  final T1 item1;
  final T2 item2;

  Tuple(this.item1, this.item2);
}

class KeyPair {
  final String original;
  final String shadow;
  KeyPair(this.original, this.shadow);
}

List<KeyPair> _distinctShadow(List<KeyPair> list) {
  if (list.isEmpty) return list;
  list.sort((a, b) => a.shadow.compareTo(b.shadow));
  var unique = <KeyPair>[];
  unique.add(list[0]);

  for (var i = 0; i < list.length; i++) {
    if (list[i].shadow != unique.last.shadow) {
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
