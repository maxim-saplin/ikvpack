part of ikvpack_core;

class IkvPackInstanceWorker extends PooledInstanceWorker {
  @override
  Future init() {
    // TODO: implement init
    throw UnimplementedError();
  }

  @override
  Future receiveRemoteCall(Action action) {
    // TODO: implement receiveRemoteCall
    throw UnimplementedError();
  }
}

class IkvPackProxy implements IkvPack {
  late PooledInstance _pi;

  @override
  Future<String> operator [](String key) {
    throw UnimplementedError();
  }

  @override
  List<String> get _kswOriginalKeys => throw UnimplementedError();

  @override
  bool containsKey(String key) {
    throw UnimplementedError();
  }

  @override
  void dispose() {}

  @override
  Future<LinkedHashMap<String, String>> getRange(
      int? startIndex, int? endIndex) {
    throw UnimplementedError();
  }

  @override
  Future<LinkedHashMap<String, Uint8List>> getRangeRaw(
      int? startIndex, int? endIndex) {
    throw UnimplementedError();
  }

  @override
  Future<Stats> getStats() {
    throw UnimplementedError();
  }

  @override
  int indexOf(String key) {
    throw UnimplementedError();
  }

  @override
  UnmodifiableListView<KeyBasket> get keyBaskets => throw UnimplementedError();

  @override
  UnmodifiableListView<String> get keys => throw UnimplementedError();

  @override
  bool get keysCaseInsensitive => throw UnimplementedError();

  @override
  List<String> keysStartingWith(String key,
      [int maxResults = 100, bool returnShadowKeys = false]) {
    throw UnimplementedError();
  }

  @override
  int get length => throw UnimplementedError();

  @override
  bool get noOutOfOrderFlag => throw UnimplementedError();

  @override
  bool get noUpperCaseFlag => throw UnimplementedError();

  @override
  Future<void> saveTo(String path,
      [Function(int progressPercent)? updateProgress]) {
    throw UnimplementedError();
  }

  @override
  UnmodifiableListView<String> get shadowKeys => throw UnimplementedError();

  @override
  bool get shadowKeysUsed => throw UnimplementedError();

  @override
  int get sizeBytes => throw UnimplementedError();

  @override
  StorageBase? get storage => throw UnimplementedError();

  @override
  Future<String> value(String key) {
    throw UnimplementedError();
  }

  @override
  Future<String> valueAt(int index) {
    throw UnimplementedError();
  }

  @override
  Future<Uint8List> valueRawCompressed(String key) {
    throw UnimplementedError();
  }

  @override
  Future<Uint8List> valueRawCompressedAt(int index) {
    throw UnimplementedError();
  }

  @override
  bool get valuesInMemory => throw UnimplementedError();
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
    var ikv = await IkvPack.load(params.path, params.keysCaseInsensitive);
    ikv.storage?.close();
    params.sendPort.send(ikv);
  } catch (e) {
    params.errorPort.send(e);
  }
}

class IkvPooledJob extends PooledJob<IkvPackData> {
  final String path;
  final bool keysCaseInsensitive;

  IkvPooledJob(this.path, this.keysCaseInsensitive);

  @override
  Future<IkvPackData> job() async {
    var ikv = await IkvPack.load(path, keysCaseInsensitive);
    ikv.storage?.close();
    var data = IkvPackData(ikv.keys, ikv.shadowKeys, ikv.keyBaskets);
    return data;
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
