import 'dart:async';
import 'dart:isolate';

abstract class PooledJob<E> {
  Future<E> job();
}

// Requests have global scope
int _reuestIdCounter = 0;
// instances are scoped to pools
int _instanceIdCounter = 0;
//TODO consider adding timeouts
Map<int, Completer> _requestCompleters = {}; // requestId is key

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
    return Future<R>(() => 0 as R);
  }

  Future<R> _sendRequest<R>(Action action) {
    var request = _Request(_instanceId, action);
    _sendPort.send(request);
    var c = Completer<R>();
    _requestCompleters[request.id] = c;
    return c.future;
  }

  /// This method is called in isolate whenever a pool receives a request to create a pooled instance
  Future createInstance();

  /// Overide this method to respond to actions
  Future<dynamic> receiveRemoteCall(Action action);
}

class _CreationResponse {
  final int _instanceId;

  /// If null - creation went successfully
  final dynamic error;

  _CreationResponse(this._instanceId, this.error);
}

enum _PooledInstanceStatus { starting, started }

class _InstanceMapEntry {
  final PooledInstance instance;
  final int isolateIndex;
  _PooledInstanceStatus state = _PooledInstanceStatus.starting;

  _InstanceMapEntry(this.instance, this.isolateIndex);
}

class IsolatePool {
  final int numberOfIsolates;
  final List<SendPort?> isolateSendPorts = [];
  final List<Isolate> isolates = [];
  final List<bool> isolateBusyWithJob = [];
  List<_PooledJobInternal> jobs = [];
  int lastJobStarted = 0;
  List<Completer> jobCompleters = [];

  final Map<int, _InstanceMapEntry> _pooledInstances = {};
  //TODO consider adding timeouts
  Map<int, Completer<PooledInstance>> creationCompleters =
      {}; // instanceId is key

  IsolatePool(this.numberOfIsolates);

  Future scheduleJob(PooledJob job) {
    jobs.add(_PooledJobInternal(job, jobs.length, -1));
    var completer = Completer();
    jobCompleters.add(completer);
    _runJobWithVacantIsolate();
    return completer.future;
  }

  Future<PooledInstance> createInstance(
      PooledInstanceWorker worker, PooledCallbak? callbak) async {
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

    isolateSendPorts[minIndex]!.send(worker);

    return completer.future;
  }

  void _runJobWithVacantIsolate() {
    var availableIsolate = isolateBusyWithJob.indexOf(false);
    if (availableIsolate > -1 && lastJobStarted < jobs.length) {
      var job = jobs[lastJobStarted];
      job.isolateIndex = availableIsolate;
      isolateSendPorts[availableIsolate]!.send(job);
      isolateBusyWithJob[availableIsolate] = true;
      lastJobStarted++;
    }
  }

  int _isolatesStarted = 0;
  // ignore: omit_local_variable_types
  double _avgMicroseconds = 0;

  Future start() async {
    print('Creating a pool of ${numberOfIsolates} running isolates');

    _isolatesStarted = 0;
    // ignore: omit_local_variable_types
    _avgMicroseconds = 0;

    var last = Completer();
    for (var i = 0; i < numberOfIsolates; i++) {
      isolateBusyWithJob.add(false);
      isolateSendPorts.add(null);
    }

    var spawnSw = Stopwatch();
    spawnSw.start();

    for (var i = 0; i < numberOfIsolates; i++) {
      var receivePort = ReceivePort();
      var sw = Stopwatch();

      sw.start();
      var params = _PooledIsolateParams(receivePort.sendPort, i, sw);

      var isolate = await Isolate.spawn<_PooledIsolateParams>(
          _pooledIsolateBody, params,
          errorsAreFatal: false);

      isolates.add(isolate);

      receivePort.listen((data) {
        if (data is _CreationResponse) {
          _processCreationResponse(data);
        } else if (data is _Request) {
          _processRequest(data);
        } else if (data is _Response) {
          _processResponse(data);
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
    isolateSendPorts[params.isolateIndex] = params.sendPort;
    _avgMicroseconds += params.stopwatch.elapsedMicroseconds;
    if (_isolatesStarted == numberOfIsolates) {
      _avgMicroseconds /= numberOfIsolates;
      print('Avg time to complete starting an isolate is '
          '${_avgMicroseconds} microseconds');
      last.complete();
    }
  }

  void _processJobResult(_PooledJobResult result) {
    isolateBusyWithJob[result.isolateIndex] = false;
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
    var pi = _pooledInstances[instanceId]!;
    if (pi.state == _PooledInstanceStatus.starting) {
      throw 'Cant send request to instance in Starting state, instanceId ${instanceId}';
    }
    var index = pi.isolateIndex;
    var request = _Request(instanceId, action);
    isolateSendPorts[index]!.send(request);
    var c = Completer<R>();
    _requestCompleters[request.id] = c;
    return c.future;
  }

  Future _processRequest(_Request request) async {
    if (!_workerInstances.containsKey(request.instanceId)) {
      print(
          'Isolate pool received request to unknown instance ${request.instanceId}');
      return;
    }
    var i = _pooledInstances[request.instanceId]!;
    if (i.instance.remoteCallback != null) {
      print(
          'Isolate pool received request to instance ${request.instanceId} which doesnt have callback intialized');
      return;
    }
    try {
      var result = i.instance.remoteCallback!(request.action);
      var response = _Response(request.id, result, null);

      isolateSendPorts[i.isolateIndex]?.send(response);
    } catch (e) {
      var response = _Response(request.id, null, e);
      isolateSendPorts[i.isolateIndex]?.send(response);
    }
  }

  void stop() {
    for (var i in isolates) {
      i.kill();
      for (var c in jobCompleters) {
        if (!c.isCompleted) {
          c.completeError('Isolate pool stopped upon request, canceling jobs');
        }
      }
    }
  }
}

void _processResponse(_Response response) {
  if (!_requestCompleters.containsKey(response.requestId)) {
    throw 'Responnse to non-existing request (id ${response.requestId}) recevied';
  }
  var c = _requestCompleters[response.requestId]!;
  if (response.error != null) {
    c.completeError(response.error);
  } else {
    c.complete(response.result);
  }
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
        // TODO, check sending 2nd request before the 1st completes
        var result = await i.receiveRemoteCall(req.action);
        var response = _Response(req.id, result, null);
        params.sendPort.send(response);
      } catch (e) {
        var response = _Response(req.id, null, e);
        params.sendPort.send(response);
      }
    } else if (message is _Response) {
      var res = message;
      if (!_requestCompleters.containsKey(res.requestId)) {
        print(
            'Isolate ${params.isolateIndex} received response to unknown request ${res.requestId}');
        return;
      }
      _processResponse(res);
    } else if (message is PooledInstanceWorker) {
      try {
        // TODO check error is propagated if instance fails to be created
        var pw = message;
        await pw.createInstance();
        pw._sendPort = params.sendPort;
        _workerInstances[message._instanceId] = message;
        var success = _CreationResponse(message._instanceId, null);
        params.sendPort.send(success);
      } catch (e) {
        var error = _CreationResponse(message._instanceId, e);
        params.sendPort.send(error);
      }
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
