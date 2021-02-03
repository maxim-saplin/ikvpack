part of ikvpack_core;

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
    ikv._storage?.close();
    params.sendPort.send(ikv);
  } catch (e) {
    params.errorPort.send(e);
  }
}

class IkvPooledJob extends PooledJob<_IkvPackData> {
  final String path;
  final bool keysCaseInsensitive;

  IkvPooledJob(this.path, this.keysCaseInsensitive);

  @override
  Future<_IkvPackData> job() async {
    var ikv = await IkvPack.load(path, keysCaseInsensitive);
    ikv._storage?.close();
    var data = _IkvPackData(ikv._originalKeys,
        ikv._shadowKeysUsed ? ikv._shadowKeys : null, ikv._keyBaskets);
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
