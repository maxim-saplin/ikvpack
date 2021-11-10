import 'dart:async';
//import 'dart:html';
import 'dart:isolate';
import 'dart:io' as P;

abstract class PooledJob<E> {
  Future<E> job();
}

// Requests have global scope
int _reuestIdCounter = 0;
// instances are scoped to pools
int _instanceIdCounter = 0;
//TODO consider adding timeouts, check fulfilled requests are deleted
Map<int, Completer> _isolateRequestCompleters = {}; // requestId is key

abstract class Action {} // holder of action type and payload

class _Request {
  final int instanceId;
  final int id;
  final Action action;
  _Request(this.instanceId, this.action) : id = _reuestIdCounter++;
}

class _Response {
  final int requestId;
  final dynamic result;
  final dynamic error;
  _Response(this.requestId, this.result, this.error);
}

typedef PooledCallbak<T> = T Function(Action action);

class PooledInstance {
  final int _instanceId;
  final IsolatePool _pool;
  PooledInstance._(this._instanceId, this._pool, this.remoteCallback);

  Future<R> callRemoteMethod<R>(Action action) {
    if (_pool.state == IsolatePoolState.stoped) {
      throw 'Isolate pool has been stoped, cant call pooled instnace method';
    }
    return _pool._sendRequest<R>(_instanceId, action);
  }

  /// If not null isolate instance can send actions back to main isolate and this callback will be called
  PooledCallbak? remoteCallback;
}

/// Subclass this type
abstract class PooledInstanceWorker {
  final int _instanceId;
  late SendPort _sendPort;
  PooledInstanceWorker() : _instanceId = _instanceIdCounter++;
  Future<R> callRemoteMethod<R>(Action action) async {
    return _sendRequest<R>(action);
  }

  Future<R> _sendRequest<R>(Action action) {
    var request = _Request(_instanceId, action);
    _sendPort.send(request);
    var c = Completer<R>();
    _isolateRequestCompleters[request.id] = c;
    return c.future;
  }

  /// This method is called in isolate whenever a pool receives a request to create a pooled instance
  Future init();

  /// Overide this method to respond to actions
  Future<dynamic> receiveRemoteCall(Action action);
}

class _CreationResponse {
  final int _instanceId;

  /// If null - creation went successfully
  final dynamic error;

  _CreationResponse(this._instanceId, this.error);
}

class _DestroyRequest {
  final int _instanceId;
  _DestroyRequest(this._instanceId);
}

enum _PooledInstanceStatus { starting, started }

class _InstanceMapEntry {
  final PooledInstance instance;
  final int isolateIndex;
  _PooledInstanceStatus state = _PooledInstanceStatus.starting;

  _InstanceMapEntry(this.instance, this.isolateIndex);
}

enum IsolatePoolState { notStarted, started, stoped }

class IsolatePool {
  final int numberOfIsolates;
  final List<SendPort?> _isolateSendPorts = [];
  final List<Isolate> _isolates = [];
  final List<bool> _isolateBusyWithJob = [];
  final List<_PooledJobInternal> _jobs = [];
  int lastJobStarted = 0;
  List<Completer> jobCompleters = [];

  IsolatePoolState _state = IsolatePoolState.notStarted;

  IsolatePoolState get state => _state;

  final Completer _started = Completer();

  /// A pool can be started early on upon app launch and checked latter and awaited to if not started yet
  Future get started => _started.future;

  final Map<int, _InstanceMapEntry> _pooledInstances = {};

  int get numberOfPooledInstances => _pooledInstances.length;
  int get numberOfPendingRequests => _requestCompleters.length;

  /// Returns the number of isolate the Pooled Instance lives in, -1 if instance is not found
  int indexOfPi(PooledInstance instance) {
    if (!_pooledInstances.containsKey(instance._instanceId)) return -1;
    return _pooledInstances[instance._instanceId]!.isolateIndex;
  }

  //TODO consider adding timeouts
  final Map<int, Completer> _requestCompleters = {}; // requestId is key
  Map<int, Completer<PooledInstance>> creationCompleters =
      {}; // instanceId is key

  IsolatePool(this.numberOfIsolates);

  Future scheduleJob(PooledJob job) {
    if (state == IsolatePoolState.stoped) {
      throw 'Isolate pool has been stoped, cant schedule a job';
    }
    _jobs.add(_PooledJobInternal(job, _jobs.length, -1));
    var completer = Completer();
    jobCompleters.add(completer);
    _runJobWithVacantIsolate();
    return completer.future;
  }

  void destroyInstance(PooledInstance instance) {
    var index = indexOfPi(instance);
    if (index == -1) {
      throw 'Cant find instance with id ${instance._instanceId} among active to destroy it';
    }

    _isolateSendPorts[index]!.send(_DestroyRequest(instance._instanceId));
    _pooledInstances.remove(instance._instanceId);
  }

  Future<PooledInstance> createInstance(PooledInstanceWorker worker,
      [PooledCallbak? callbak]) async {
    var pi = PooledInstance._(worker._instanceId, this, callbak);

    var min = 10000000;
    var minIndex = 0;

    for (var i = 0; i < numberOfIsolates; i++) {
      var x = _pooledInstances.entries
          .where((e) => e.value.isolateIndex == i)
          .fold(0, (int previousValue, _) => previousValue + 1);
      if (x < min) {
        min = x;
        minIndex = i;
      }
    }

    _pooledInstances[pi._instanceId] = _InstanceMapEntry(pi, minIndex);

    var completer = Completer<PooledInstance>();
    creationCompleters[pi._instanceId] = completer;

    _isolateSendPorts[minIndex]!.send(worker);

    return completer.future;
  }

  void _runJobWithVacantIsolate() {
    var availableIsolate = _isolateBusyWithJob.indexOf(false);
    if (availableIsolate > -1 && lastJobStarted < _jobs.length) {
      var job = _jobs[lastJobStarted];
      job.isolateIndex = availableIsolate;
      _isolateSendPorts[availableIsolate]!.send(job);
      _isolateBusyWithJob[availableIsolate] = true;
      lastJobStarted++;
    }
  }

  int _isolatesStarted = 0;
  // ignore: omit_local_variable_types
  double _avgMicroseconds = 0;

  static final Uri _uri = Uri.parse(
      'package:ikvpack/src/isolate_helpers.dart'); //P.Platform.script;

  Future start() async {
    print('Creating a pool of $numberOfIsolates running isolates');

    _isolatesStarted = 0;
    // ignore: omit_local_variable_types
    _avgMicroseconds = 0;

    var last = Completer();
    for (var i = 0; i < numberOfIsolates; i++) {
      _isolateBusyWithJob.add(false);
      _isolateSendPorts.add(null);
    }

    var spawnSw = Stopwatch();
    spawnSw.start();

    for (var i = 0; i < numberOfIsolates; i++) {
      var receivePort = ReceivePort();
      var sw = Stopwatch();

      sw.start();
      var params = _PooledIsolateParams(receivePort.sendPort, i, sw);

      // var isolate = await Isolate.spawn<_PooledIsolateParams>(
      //     _pooledIsolateBody, params,
      //     errorsAreFatal: false);

      //print('URI ${_uri}');

      var isolate = await Isolate.spawnUri(
          _uri, [params.isolateIndex.toString()], params.sendPort,
          errorsAreFatal: false);

      _isolates.add(isolate);

      receivePort.listen((data) {
        if (data is _CreationResponse) {
          _processCreationResponse(data);
        } else if (data is _Request) {
          _processRequest(data);
        } else if (data is _Response) {
          _processResponse(data, _requestCompleters);
        } else if (data is _PooledIsolateParams) {
          _processIsolateStartResult(data, last);
        } else if (data is _PooledJobResult) {
          _processJobResult(data);
        }
      });
    }

    spawnSw.stop();

    print('spawn() called on ${numberOfIsolates} isolates'
        '(${spawnSw.elapsedMicroseconds} microseconds)');

    return last.future;
  }

  void _processCreationResponse(_CreationResponse r) {
    if (!creationCompleters.containsKey(r._instanceId)) {
      print('Invalid _instanceId ${r._instanceId} receivd in _CreationRepnse');
    } else {
      var c = creationCompleters[r._instanceId]!;
      if (r.error != null) {
        c.completeError(r.error);
        creationCompleters.remove(r._instanceId);
        _pooledInstances.remove(r._instanceId);
      } else {
        c.complete(_pooledInstances[r._instanceId]!.instance);
        creationCompleters.remove(r._instanceId);
        _pooledInstances[r._instanceId]!.state = _PooledInstanceStatus.started;
      }
    }
  }

  void _processIsolateStartResult(_PooledIsolateParams params, Completer last) {
    _isolatesStarted++;
    _isolateSendPorts[params.isolateIndex] = params.sendPort;
    _avgMicroseconds += params.stopwatch.elapsedMicroseconds;
    if (_isolatesStarted == numberOfIsolates) {
      _avgMicroseconds /= numberOfIsolates;
      print('Avg time to complete starting an isolate is '
          '${_avgMicroseconds} microseconds');
      last.complete();
      _started.complete();
      _state = IsolatePoolState.started;
    }
  }

  void _processJobResult(_PooledJobResult result) {
    _isolateBusyWithJob[result.isolateIndex] = false;
    if (result.error == null) {
      jobCompleters[result.jobIndex].complete(result.result);
    } else {
      jobCompleters[result.jobIndex].completeError(result.error);
    }
    _runJobWithVacantIsolate();
  }

  Future<R> _sendRequest<R>(int instanceId, Action action) {
    if (!_pooledInstances.containsKey(instanceId)) {
      throw 'Cant send request to non-existing instance, instanceId ${instanceId}';
    }
    var pim = _pooledInstances[instanceId]!;
    if (pim.state == _PooledInstanceStatus.starting) {
      throw 'Cant send request to instance in Starting state, instanceId ${instanceId}';
    }
    var index = pim.isolateIndex;
    var request = _Request(instanceId, action);
    _isolateSendPorts[index]!.send(request);
    var c = Completer<R>();
    _requestCompleters[request.id] = c;
    return c.future;
  }

  Future _processRequest(_Request request) async {
    if (!_pooledInstances.containsKey(request.instanceId)) {
      print(
          'Isolate pool received request to unknown instance ${request.instanceId}');
      return;
    }
    var i = _pooledInstances[request.instanceId]!;
    if (i.instance.remoteCallback == null) {
      print(
          'Isolate pool received request to instance ${request.instanceId} which doesnt have callback intialized');
      return;
    }
    try {
      var result = i.instance.remoteCallback!(request.action);
      var response = _Response(request.id, result, null);

      _isolateSendPorts[i.isolateIndex]?.send(response);
    } catch (e) {
      var response = _Response(request.id, null, e);
      _isolateSendPorts[i.isolateIndex]?.send(response);
    }
  }

  void stop() {
    for (var i in _isolates) {
      i.kill();
      for (var c in jobCompleters) {
        if (!c.isCompleted) {
          c.completeError('Isolate pool stopped upon request, cancelling jobs');
        }
      }
      for (var c in creationCompleters.values) {
        if (!c.isCompleted) {
          c.completeError(
              'Isolate pool stopped upon request, cancelling instance creation requests');
        }
      }
      creationCompleters.clear();
      for (var c in _requestCompleters.values) {
        if (!c.isCompleted) {
          c.completeError(
              'Isolate pool stopped upon request, cancelling pending request');
        }
      }
      _requestCompleters.clear();
    }

    _state = IsolatePoolState.stoped;
  }
}

void _processResponse(_Response response,
    [Map<int, Completer>? requestCompleters]) {
  var cc = requestCompleters ?? _isolateRequestCompleters;
  if (!cc.containsKey(response.requestId)) {
    throw 'Responnse to non-existing request (id ${response.requestId}) recevied';
  }
  var c = cc[response.requestId]!;
  if (response.error != null) {
    c.completeError(response.error);
  } else {
    c.complete(response.result);
  }
  cc.remove(response.requestId);
}

class _PooledIsolateParams<E> {
  final SendPort sendPort;
  final int isolateIndex;
  final Stopwatch stopwatch;

  _PooledIsolateParams(this.sendPort, this.isolateIndex, this.stopwatch);
}

class _PooledJobInternal {
  _PooledJobInternal(this.job, this.jobIndex, this.isolateIndex);
  final PooledJob job;
  final int jobIndex;
  int isolateIndex;
}

class _PooledJobResult {
  _PooledJobResult(this.result, this.jobIndex, this.isolateIndex);
  final dynamic result;
  final int jobIndex;
  final int isolateIndex;
  dynamic error;
}

var _workerInstances = <int, PooledInstanceWorker>{};

// For running in separate isolate group
void main(args, message) {
  print(args[0]);
  var pip = _PooledIsolateParams(
      message as SendPort, int.parse(args[0]), Stopwatch());
  _pooledIsolateBody(pip);
}

void _pooledIsolateBody(_PooledIsolateParams params) async {
  params.stopwatch.stop();
  print(
      'Isolate #${params.isolateIndex} started (${params.stopwatch.elapsedMicroseconds} microseconds)');
  var isolatePort = ReceivePort();
  params.sendPort.send(_PooledIsolateParams(
      isolatePort.sendPort, params.isolateIndex, params.stopwatch));

  _reuestIdCounter = 1000000000 *
      (params.isolateIndex +
          1); // split counters into ranges to deal with overlaps between isolates. Theoretically after one billion requests a collision might happen
  isolatePort.listen((message) async {
    if (message is _Request) {
      var req = message;
      if (!_workerInstances.containsKey(req.instanceId)) {
        print(
            'Isolate ${params.isolateIndex} received request to unknown instance ${req.instanceId}');
        return;
      }
      var i = _workerInstances[req.instanceId]!;
      try {
        var result = await i.receiveRemoteCall(req.action);
        var response = _Response(req.id, result, null);
        params.sendPort.send(response);
      } catch (e) {
        var response = _Response(req.id, null, e);
        params.sendPort.send(response);
      }
    } else if (message is _Response) {
      var res = message;
      if (!_isolateRequestCompleters.containsKey(res.requestId)) {
        print(
            'Isolate ${params.isolateIndex} received response to unknown request ${res.requestId}');
        return;
      }
      _processResponse(res);
    } else if (message is PooledInstanceWorker) {
      try {
        var pw = message;
        await pw.init();
        pw._sendPort = params.sendPort;
        _workerInstances[message._instanceId] = message;
        var success = _CreationResponse(message._instanceId, null);
        params.sendPort.send(success);
      } catch (e) {
        var error = _CreationResponse(message._instanceId, e);
        params.sendPort.send(error);
      }
    } else if (message is _DestroyRequest) {
      if (!_workerInstances.containsKey(message._instanceId)) {
        print(
            'Isolate ${params.isolateIndex} received destroy request of unknown instance ${message._instanceId}');
        return;
      }
      _workerInstances.remove(message._instanceId);
    } else if (message is _PooledJobInternal) {
      try {
        // params.stopwatch.reset();
        // params.stopwatch.start();
        //print('Job index ${message.jobIndex}');

        var result = await message.job.job();
        // params.stopwatch.stop();
        // print('Job done in ${params.stopwatch.elapsedMilliseconds} ms');
        // params.stopwatch.reset();
        // params.stopwatch.start();
        params.sendPort.send(
            _PooledJobResult(result, message.jobIndex, message.isolateIndex));
        // params.stopwatch.stop();
        // print('Job result sent in ${params.stopwatch.elapsedMilliseconds} ms');
      } catch (e) {
        var r = _PooledJobResult(null, message.jobIndex, message.isolateIndex);
        r.error = e;
        params.sendPort.send(r);
      }
    }
  });
}

class _IsolateCallbackArg<A> {
  final A value;
  _IsolateCallbackArg(this.value);
}

abstract class CallbackIsolateJob<R, A> {
  final bool synchronous;
  CallbackIsolateJob(this.synchronous);
  Future<R> jobAsync();
  R jobSync();
  void sendDataToCallback(A arg) {
    // Wrap arg in _IsolateCallbackArg to avoid cases when A == E
    _sendPort!.send(_IsolateCallbackArg<A>(arg));
  }

  SendPort? _sendPort;
  SendPort? _errorPort;
}

class CallbackIsolate<R, A> {
  final CallbackIsolateJob<R, A> job;
  CallbackIsolate(this.job);
  Future<R> run(Function(A arg)? callback) async {
    var completer = Completer<R>();
    var receivePort = ReceivePort();
    var errorPort = ReceivePort();

    job._sendPort = receivePort.sendPort;
    job._errorPort = errorPort.sendPort;

    var isolate = await Isolate.spawn<CallbackIsolateJob<R, A>>(
        _isolateBody, job,
        errorsAreFatal: true);

    receivePort.listen((data) {
      if (data is _IsolateCallbackArg) {
        if (callback != null) callback(data.value);
      } else if (data is R) {
        completer.complete(data);
        isolate.kill();
      }
    });

    errorPort.listen((e) {
      completer.completeError(e);
    });

    return completer.future;
  }
}

void _isolateBody(CallbackIsolateJob job) async {
  try {
    //print('Job index ${message.jobIndex}');
    var result = job.synchronous ? job.jobSync() : await job.jobAsync();
    job._sendPort!.send(result);
  } catch (e) {
    job._errorPort!.send(e);
  }
}
