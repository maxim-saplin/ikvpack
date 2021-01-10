import 'dart:collection';
import 'dart:convert';
import 'dart:typed_data';
import 'package:archive/archive.dart';
import 'storage_vm.dart'
    if (dart.library.io) 'storage_vm.dart'
    if (dart.library.html) 'storage_web.dart';

class IkvPack {
  final Storage? _storage;

  IkvPack(String path, [this.keysCaseInsensitive = true])
      : _valuesInMemory = false,
        _storage = Storage(path) {
    _keysList = (_storage as Storage).readSortedKeys();
    if (keysCaseInsensitive) {
      _keysLowerCase = List.generate(_keysList.length,
          (i) => _Triple._fixOutOfOrder(_keysList[i].toLowerCase()),
          growable: false);
    }
    _keysReadOnly = UnmodifiableListView<String>(_keysList);

    _buildBaskets();
  }

  /// Do not do strcit comparisons by ignoring case.
  /// Make lowercase shadow version of keys and uses those for lookups.
  /// Also fix out of-order-chars, e.g. replace cyrylic 'ё' (code 1110 which in alphabet stands before 'я', code 1103)
  /// with 'е' (code 1077)
  bool keysCaseInsensitive = true;

  List<String> _keysList = [];
  // TODO, Test performance/mem consumption, maybe make 5-10 char lower case keys index for fast searches, not full keys
  List<String> _keysLowerCase = [];
  final List<_KeyBasket> _keyBaskets = [];
  List<List<int>> _values = [];
  final ZLibDecoder decoder = ZLibDecoder();

  UnmodifiableListView<String> _keysReadOnly = UnmodifiableListView<String>([]);
  UnmodifiableListView<String> get keys => _keysReadOnly;

  /// Web implementation does not support indexed keys
  //bool get indexedKeys => true;

  final bool _valuesInMemory;

  /// Default constructor reads keys into memory while access file (or Indexed DB in Web)
  /// when a value is needed. fromMap() constructor stores everythin in memory
  bool get valuesInMemory => _valuesInMemory;

  /// String values are compressed via Zlib
  IkvPack.fromMap(Map<String, String> map, [this.keysCaseInsensitive = true])
      : _valuesInMemory = true,
        _storage = null {
    var entries = _getSortedEntries(map);

    var enc = ZLibEncoder();

    _keysList =
        List.generate(entries.length, (i) => entries[i].key, growable: false);
    if (keysCaseInsensitive) {
      _keysLowerCase = List.generate(
          entries.length, (i) => entries[i].keyLowerCase,
          growable: false);
    }
    _values = List.generate(entries.length, (i) {
      var utf = utf8.encode(entries[i].value);
      var zip = enc.encode(utf);
      return zip;
    }, growable: false);

    _keysReadOnly = UnmodifiableListView<String>(_keysList);
    _buildBaskets();
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

  // TODO test 1 key, 3 keys 'a', 'b' and 'c'
  void _buildBaskets() {
    var list = _keysList;
    if (keysCaseInsensitive) list = _keysLowerCase;

    var firstLetter = list[0][0];
    var index = 0;

    if (list.length == 1) {
      _keyBaskets.add(_KeyBasket(firstLetter, index, index));
      return;
    }

    for (var i = 1; i < list.length; i++) {
      var newFirstLetter = list[i][0];
      if (newFirstLetter != firstLetter) {
        _keyBaskets.add(_KeyBasket(firstLetter, index, i - 1));
        firstLetter = newFirstLetter;
        index = i;
      }
    }

    _keyBaskets.add(_KeyBasket(firstLetter, index, list.length - 1));
  }

  /// Serilized object to given file on VM and IndexedDB in Web
  void saveTo(String path) {
    saveToPath(path, _keysList, _values);
  }

  int get length => _keysList.length;

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  String valueAt(int index) {
    var bytes = valuesInMemory
        ? decoder.decodeBytes(_values[index])
        : decoder.decodeBytes((_storage as Storage).valueAt(index));
    var value = utf8.decode(bytes, allowMalformed: true);
    return value;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Uint8List valueRawCompressedAt(int index) {
    var value =
        valuesInMemory ? _values[index] : (_storage as Storage).valueAt(index);

    return Uint8List.fromList(value);
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  String value(String key) {
    var index = indexOf(key);
    if (index < 0) throw 'key not foiund';

    return _storage != null && !(_storage as Storage).useIndexToGetValue
        ? utf8.decode(decoder.decodeBytes((_storage as Storage).value(key)),
            allowMalformed: true)
        : valueAt(index);
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  Uint8List valueRawCompressed(String key) {
    var index = indexOf(key);
    if (index < 0) throw 'key not foiund';

    return _storage != null && !(_storage as Storage).useIndexToGetValue
        ? Uint8List.fromList((_storage as Storage).value(key))
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
    var list = _keysList;
    if (keysCaseInsensitive) {
      key = key.toLowerCase();
      list = _keysLowerCase;
    }

    for (var b in _keyBaskets) {
      if (b.firstLetter == key[0]) {
        var l = b.startIndex;
        var r = b.endIndex;
        while (l <= r) {
          var m = (l + (r - l) / 2).round();

          var res = key.compareTo(list[m]);

          // Check if x is present at mid
          if (res == 0) return m;

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

    return -1;
  }

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  bool containsKey(String key) => indexOf(key) > -1;

  List<String> keysStartingWith(String value, [int maxResult = 100]) {
    var keys = <String>[];
    var list = _keysList;
    value = value.trim();

    if (keysCaseInsensitive) {
      value = value.toLowerCase();
      value = _Triple._fixOutOfOrder(
          value); // TODO, add test 'имгненне' finds 'iмгненне', bug which becomes feature (allow searching BY words with RU substitues)
      list = _keysLowerCase;
    }

    for (var b in _keyBaskets) {
      if (b.firstLetter == value[0]) {
        for (var i = b.startIndex; i <= b.endIndex; i++) {
          if (list[i].startsWith(value)) {
            keys.add(list[i]);
            if (keys.length >= maxResult) return keys;
          }
        }
      }
    }
    return keys;
  }

  void dispose() {
    _storage?.dispose();
  }
}

// class KeysAndValues {
//   final List<String> keys;
//   final List<List<int>> values;

//   KeysAndValues(this.keys, this.values);
// }

abstract class StorageBase {
  StorageBase(String path);

  List<String> readSortedKeys();
  List<int> value(String key);
  List<int> valueAt(int index);
  void dispose();
  bool get useIndexToGetValue;
}

class _KeyBasket {
  final String firstLetter;
  final int startIndex;
  final int endIndex;

  _KeyBasket(this.firstLetter, this.startIndex, this.endIndex);
}

class _Triple {
  final String key;
  final String keyLowerCase;
  final String value;

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
  static String _fixOutOfOrder(String value) {
    value =
        value.replaceAll('ё', 'е').replaceAll('і', 'и').replaceAll('ў', 'у');
    return value;
  }

  _Triple(this.key, this.value)
      : keyLowerCase = _fixOutOfOrder(key.toLowerCase());

  _Triple.noLowerCase(this.key, this.value) : keyLowerCase = '';
}
