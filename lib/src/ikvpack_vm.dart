import '../ikvpack.dart';

class IkvPack extends IkvPackBase {
  IkvPack(String path) : super(path);

  IkvPack.fromStringMap(Map<String, String> map) : super.fromStringMap(map);

  @override
  // TODO: implement indexedKeys
  bool get indexedKeys => true;

  @override
  void packToFile(String path) {
    // TODO: implement packToFile
  }
}
