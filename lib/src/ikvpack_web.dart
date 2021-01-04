import 'dart:collection';
import 'dart:typed_data';
import '../ikvpack.dart';

class IkvPack extends IkvPackAbsrtact {
  IkvPack.fromMap(Map<String, Uint8List> map) : super.fromMap(map);

  @override
  // TODO: implement indexedKeys
  bool get indexedKeys => throw UnimplementedError();

  @override
  // TODO: implement keys
  UnmodifiableListView<String> get keys => throw UnimplementedError();

  @override
  Iterable<String> keysStartingWith(String value, [int maxResult = 100]) {
    // TODO: implement keysStartingWith
    throw UnimplementedError();
  }

  @override
  void packToFile(String path) {
    // TODO: implement packToFile
  }

  @override
  Uint8List value(String key) {
    // TODO: implement value
    throw UnimplementedError();
  }

  @override
  Uint8List valueAt(int index) {
    // TODO: implement valueAt
    throw UnimplementedError();
  }
}
