export './ikvpack_vm.dart'
    if (dart.library.io) './ikvpack_vm.dart'
    if (dart.library.html) './ikvpack_web.dart';

import 'dart:collection';
import 'dart:typed_data';

abstract class IkvPackAbsrtact {
  IkvPackAbsrtact(String path);
  IkvPackAbsrtact.fromMap(Map<String, Uint8List> map);

  void packToFile(String path);

  UnmodifiableListView<String> get keys;
  Uint8List valueAt(int index);
  Uint8List value(String key);

  Uint8List operator [](String key) {
    return value(key);
  }

  bool get compressed => true;

  /// Web implementation does not support indexed keys
  bool get indexedKeys;

  Iterable<String> keysStartingWith(String value, [int maxResult = 100]);
}
