import 'dart:collection';
import 'dart:convert';
import 'package:archive/archive.dart';

class _KeyBasket {
  final String firstLetter;
  final int startIndex;
  final int endIndex;

  _KeyBasket(this.firstLetter, this.startIndex, this.endIndex);
}

abstract class IkvPackBase {
  IkvPackBase(String path);

  /// Make keys lowercase while building internal keys and while looking up
  bool useLowerCaseKeys = true;

  /// String values are compressed via Zlib
  IkvPackBase.fromStringMap(Map<String, String> map,
      [this.useLowerCaseKeys = true]) {
    var entries = _getSortedEntries(map);

    var enc = ZLibEncoder();

    _keysList =
        List.generate(entries.length, (i) => entries[i].key, growable: false);
    _values = List.generate(entries.length, (i) {
      var utf = utf8.encode(entries[i].value);
      var zip = enc.encode(utf);
      return zip;
    }, growable: false);

    _keysReadOnly = UnmodifiableListView<String>(_keysList);
    _buildBaskets();
  }

  List<MapEntry<String, String>> _getSortedEntries(Map<String, String> map) {
    assert(map.isNotEmpty, 'Key/Value map can\'t be empty');

    var entries = map.entries.toList();

    entries = _fixeKeys(entries);

    assert(entries.isNotEmpty, 'Keys can\'t contain only empty strings');

    entries.sort((e1, e2) => e1.key.compareTo(e2.key));

    return entries;
  }

  List<MapEntry<String, String>> _fixeKeys(List<MapEntry> entries) {
    var fixed = <MapEntry<String, String>>[];

    for (var e in entries) {
      var s = e.key;
      s = s.replaceAll('\n', '');
      s = s.replaceAll('\r', '');

      if (s.length > 1) {
        if (s.length > 255) s = s.substring(0, 255);
        if (useLowerCaseKeys) s = s.toLowerCase();
        fixed.add(MapEntry(s, e.value));
      }
    }

    return fixed;
  }

  // TODO test 1 key, 3 keys 'a', 'b' and 'c'
  void _buildBaskets() {
    var firstLetter = _keysList[0][0];
    var index = 0;

    if (_keysList.length == 1) {
      _keyBaskets.add(_KeyBasket(firstLetter, index, index));
      return;
    }

    for (var i = 1; i < _keysList.length; i++) {
      var newFirstLetter = _keysList[i][0];
      if (newFirstLetter != firstLetter) {
        _keyBaskets.add(_KeyBasket(firstLetter, index, i - 1));
        firstLetter = newFirstLetter;
        index = i;
      }
    }

    _keyBaskets.add(_KeyBasket(firstLetter, index, _keysList.length - 1));
  }

  void packToFile(String path);

  List<String> _keysList = [];
  final List<_KeyBasket> _keyBaskets = [];
  List<List<int>> _values = [];
  final ZLibDecoder decoder = ZLibDecoder();

  UnmodifiableListView<String> _keysReadOnly = UnmodifiableListView<String>([]);

  UnmodifiableListView<String> get keys => _keysReadOnly;

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  String valueAt(int index) {
    var bytes = decoder.decodeBytes(_values[index]);
    var value = utf8.decode(bytes, allowMalformed: true);
    return value;
  }

  /// -1 if not found
  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  int indexOf(String key) {
    for (var b in _keyBaskets) {
      if (b.firstLetter == key[0]) {
        var l = b.startIndex;
        var r = b.endIndex;
        while (l <= r) {
          var m = (l + (r - l) / 2).round();

          var res = key.compareTo(_keysList[m]);

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
  String value(String key) {
    var index = indexOf(key);
    if (index < 0) throw 'key not foiund';

    return valueAt(index);
  }

  /// Returns decompressed value
  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  String operator [](String key) {
    return value(key);
  }

  bool get compressed => true;

  /// Web implementation does not support indexed keys
  bool get indexedKeys;

  Iterable<String> keysStartingWith(String value, [int maxResult = 100]) {
    var keys = <String>[];

    if (useLowerCaseKeys) value = value.toLowerCase();

    for (var b in _keyBaskets) {
      if (b.firstLetter == value[0]) {
        for (var i = b.startIndex; i < b.endIndex; i++) {
          if (_keysList[i].startsWith(value)) {
            keys.add(_keysList[i]);
            if (keys.length >= maxResult) return keys;
          }
        }
      }
    }
    return keys;
  }
}
