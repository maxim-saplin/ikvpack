part of 'ikvpack_core.dart';

class IkvPackInstanceWorker extends PooledInstance {
  late IkvPack _ikv;

  @override
  Future init() async {
    return;
  }

  @override
  Future<dynamic> receiveRemoteCall(Action action) async {
    switch (action) {
      case LoadAction(:var path, :var keysCaseInsensitive):
        _ikv = await IkvPack.load(path, keysCaseInsensitive);
        return LoadActionResult(_ikv.length, _ikv.sizeBytes,
            _ikv.shadowKeysUsed, _ikv.noOutOfOrderFlag, _ikv.noUpperCaseFlag);
      case GetRangeAction(:var startIndex, :var endIndex):
        return _ikv.getRange(startIndex, endIndex);
      case GetRangeRawAction(:var startIndex, :var endIndex):
        return _ikv.getRangeRaw(startIndex, endIndex);
      case GetStatsAction():
        return _ikv.getStats();
      case KeysStartingWithAction(:var key, :var maxResults):
        return _ikv.keysStartingWith(key, maxResults);
      case SaveToAction(:var path):
        return _ikv.saveTo(path);
      case ValueAction(:var key):
        return _ikv.getValue(key);
      case ValuesAction(:var key):
        return _ikv.getValues(key);
      case ValueAtAction(:var index):
        return _ikv.getValueAt(index);
      case ValueRawConpressedAction(:var key):
        return _ikv.getValueRawCompressed(key);
      case ValueRawConpressedAtAction(:var index):
        return _ikv.getValueRawCompressedAt(index);
      default:
        throw 'Unknown action ${action.runtimeType}';
    }
  }
}

class LoadActionResult {
  final int length;
  final int sizeBytes;
  final bool shadowKeysUsed;
  final bool noOutOfOrderFlag;
  final bool noUpperCaseFlag;

  LoadActionResult(this.length, this.sizeBytes, this.shadowKeysUsed,
      this.noOutOfOrderFlag, this.noUpperCaseFlag);
}

class LoadAction extends Action {
  final String path;
  final bool keysCaseInsensitive;

  LoadAction(this.path, this.keysCaseInsensitive);
}

class GetRangeAction extends Action {
  final int? startIndex;
  final int? endIndex;

  GetRangeAction(this.startIndex, this.endIndex);
}

class GetRangeRawAction extends Action {
  final int? startIndex;
  final int? endIndex;

  GetRangeRawAction(this.startIndex, this.endIndex);
}

class GetStatsAction extends Action {}

class KeysStartingWithAction extends Action {
  final String key;
  final int maxResults;

  KeysStartingWithAction(this.key, this.maxResults);
}

class SaveToAction extends Action {
  final String path;

  SaveToAction(this.path);
}

class SaveToCallbackAction extends Action {
  final int progressPercent;

  SaveToCallbackAction(this.progressPercent);
}

class ValueAction extends Action {
  final String key;

  ValueAction(this.key);
}

class ValuesAction extends Action {
  final String key;

  ValuesAction(this.key);
}

class ValueAtAction extends Action {
  final int index;

  ValueAtAction(this.index);
}

class ValueRawConpressedAction extends Action {
  final String key;

  ValueRawConpressedAction(this.key);
}

class ValueRawConpressedAtAction extends Action {
  final int index;

  ValueRawConpressedAtAction(this.index);
}

/// IkvPackProxy uses IsolatePool to create IkvPackImpl in separate isolates and
/// communicates with them across isolate boundaries via sort of RPC.
/// This approach boost performance for when multiple large IkvPacks need to be quickly loaded for file.
/// Simply loading IkvPack (40+ MBs, 100k+ keys) in spawned isolate and returning it to main one proved
/// to be very slow due to siginificant overhead when serializing the whole object.
/// Also in Flutter apps when IkvPack was transmitted back from and Isolate
/// the main thread could hang for second or two which lead to noticable UI freezes.
/// Please not that not all APIs of IkvPack are implemented
class IkvPackProxy implements IkvPack {
  IkvPackProxy._(String path, [this._keysCaseInsensitive = true])
      : _valuesInMemory = false;

  late PooledInstanceProxy _pi;

  static Future<IkvPack> loadInIsolatePoolAndUseProxy(
      IsolatePool pool, String path,
      [keysCaseInsensitive = true]) async {
    var ikv = IkvPackProxy._(path, keysCaseInsensitive);
    ikv._pi = await pool.addInstance(IkvPackInstanceWorker(), (action) {
      switch (action.runtimeType) {
        case SaveToCallbackAction _:
          return ikv._saveToCallback != null
              ? ikv._saveToCallback!(
                  (action as SaveToCallbackAction).progressPercent)
              : null;
      }
    });

    var r = await ikv._pi.callRemoteMethod<LoadActionResult>(
        LoadAction(path, keysCaseInsensitive));
    ikv._valuesInMemory = false;
    ikv._length = r.length;
    ikv._sizeBytes = r.sizeBytes;
    ikv._shadowKeysUsed = r.shadowKeysUsed;
    ikv._noOutOfOrderFlag = r.noOutOfOrderFlag;
    ikv._noUpperCaseFlag = r.noUpperCaseFlag;

    return ikv;
  }

  final bool _keysCaseInsensitive;
  @override
  bool get keysCaseInsensitive => _keysCaseInsensitive;

  bool _shadowKeysUsed = false;
  @override
  bool get shadowKeysUsed => _shadowKeysUsed;

  bool _valuesInMemory = false;
  @override
  bool get valuesInMemory => _valuesInMemory;

  bool _noOutOfOrderFlag = false;
  @override
  bool get noOutOfOrderFlag => _noOutOfOrderFlag;

  bool _noUpperCaseFlag = false;
  @override
  bool get noUpperCaseFlag => _noUpperCaseFlag;

  int _length = -1;
  @override
  int get length => _length;

  int _sizeBytes = -1;
  @override
  int get sizeBytes => _sizeBytes;

  @pragma('vm:prefer-inline')
  @pragma('dart2js:tryInline')
  @override
  Future<String> operator [](String key) async {
    return getValue(key);
  }

  @override
  bool containsKey(String key) {
    throw UnimplementedError();
  }

  @override
  void dispose() {
    throw UnimplementedError();
  }

  @override
  Future<LinkedHashMap<String, String>> getRange(
      int? startIndex, int? endIndex) {
    return _pi.callRemoteMethod<LinkedHashMap<String, String>>(
        GetRangeAction(startIndex, endIndex));
  }

  @override
  Future<LinkedHashMap<String, Uint8List>> getRangeRaw(
      int? startIndex, int? endIndex) {
    return _pi.callRemoteMethod<LinkedHashMap<String, Uint8List>>(
        GetRangeRawAction(startIndex, endIndex));
  }

  @override
  Future<Stats> getStats() {
    return _pi.callRemoteMethod<Stats>(GetStatsAction());
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
  Future<List<KeyPair>> keysStartingWith(String key, [int maxResults = 100]) {
    return _pi.callRemoteMethod<List<KeyPair>>(
        KeysStartingWithAction(key, maxResults));
  }

  Function(int progressPercent)? _saveToCallback;

  @override
  Future<void> saveTo(String path,
      [Function(int progressPercent)? updateProgress]) async {
    _saveToCallback = updateProgress;
    var r = await _pi.callRemoteMethod(SaveToAction(path));
    _saveToCallback = null;

    return r;
  }

  @override
  UnmodifiableListView<String> get shadowKeys => throw UnimplementedError();

  @override
  StorageBase? get storage => throw UnimplementedError();

  @override
  Future<String> getValue(String key) {
    return _pi.callRemoteMethod<String>(ValueAction(key));
  }

  @override
  Future<List<String>> getValues(String key) {
    return _pi.callRemoteMethod<List<String>>(ValuesAction(key));
  }

  @override
  Future<String> getValueAt(int index) {
    return _pi.callRemoteMethod<String>(ValueAtAction(index));
  }

  @override
  Future<Uint8List> getValueRawCompressed(String key) {
    return _pi.callRemoteMethod<Uint8List>(ValueRawConpressedAction(key));
  }

  @override
  Future<Uint8List> getValueRawCompressedAt(int index) {
    return _pi.callRemoteMethod<Uint8List>(ValueRawConpressedAtAction(index));
  }
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
