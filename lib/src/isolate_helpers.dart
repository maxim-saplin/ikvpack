import 'dart:async';
import 'dart:isolate';

abstract class PooledJob<E> {
  Future<E> job();
}

class IsolatePool {
  final int numberOfIsolates;
  final List<SendPort?> isolateSendPorts = [];
  final List<Isolate> isolates = [];
  final List<bool> isolateBusy = [];
  List<_PooledJob> jobs = [];
  int lastJobStarted = 0;
  List<Completer> jobCompleters = [];

  IsolatePool(this.numberOfIsolates);

  Future scheduleJob(PooledJob job) {
    jobs.add(_PooledJob(job, jobs.length, -1));
    var completer = Completer();
    jobCompleters.add(completer);
    _runJobWithVacantIsolate();
    return completer.future;
  }

  void _runJobWithVacantIsolate() {
    var availableIsolate = isolateBusy.indexOf(false);
    if (availableIsolate > -1 && lastJobStarted < jobs.length) {
      var job = jobs[lastJobStarted];
      job.isolateIndex = availableIsolate;
      isolateSendPorts[availableIsolate]!.send(job);
      isolateBusy[availableIsolate] = true;
      lastJobStarted++;
    }
  }

  Future start() async {
    print('Creating a pool of ${numberOfIsolates} running isolates');
    var isolatesStarted = 0;
    // ignore: omit_local_variable_types
    double avgMicroseconds = 0;

    var last = Completer();
    for (var i = 0; i < numberOfIsolates; i++) {
      isolateBusy.add(false);
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
          errorsAreFatal: true);

      isolates.add(isolate);

      receivePort.listen((data) {
        if (data is _PooledIsolateParams) {
          isolatesStarted++;
          isolateSendPorts[data.isolateIndex] = data.sendPort;
          avgMicroseconds += data.stopwatch.elapsedMicroseconds;
          if (isolatesStarted == numberOfIsolates) {
            avgMicroseconds /= numberOfIsolates;
            print('Avg time to complete starting an isolate is '
                '${avgMicroseconds} microseconds');
            last.complete();
          }
        } else if (data is _PooledJobResult) {
          isolateBusy[data.isolateIndex] = false;
          if (data.error == null) {
            jobCompleters[data.jobIndex].complete(data.result);
          } else {
            jobCompleters[data.jobIndex].completeError(data.error);
          }
          _runJobWithVacantIsolate();
        } else {
          last.completeError(
              'Isolate recevied unexpected input: ${data.runtimeType}');
        }
      });
    }

    spawnSw.stop();

    print('spawn() called on ${numberOfIsolates} isolates'
        '(${spawnSw.elapsedMicroseconds} microseconds)');

    return last.future;
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

class _PooledIsolateParams<E> {
  final SendPort sendPort;
  final int isolateIndex;
  final Stopwatch stopwatch;

  _PooledIsolateParams(this.sendPort, this.isolateIndex, this.stopwatch);
}

class _PooledJob {
  _PooledJob(this.job, this.jobIndex, this.isolateIndex);
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

void _pooledIsolateBody(_PooledIsolateParams params) async {
  params.stopwatch.stop();
  print(
      'Isolate #${params.isolateIndex} started (${params.stopwatch.elapsedMicroseconds} microseconds)');
  var isolatePort = ReceivePort();
  params.sendPort.send(_PooledIsolateParams(
      isolatePort.sendPort, params.isolateIndex, params.stopwatch));
  isolatePort.listen((message) async {
    if (message is _PooledJob) {
      try {
        params.stopwatch.reset();
        params.stopwatch.start();
        //print('Job index ${message.jobIndex}');

        var result = await message.job.job();
        params.stopwatch.stop();
        print('Job done in ${params.stopwatch.elapsedMilliseconds} ms');
        params.stopwatch.reset();
        params.stopwatch.start();
        params.sendPort.send(
            _PooledJobResult(result, message.jobIndex, message.isolateIndex));
        params.stopwatch.stop();
        print('Job result sent in ${params.stopwatch.elapsedMilliseconds} ms');
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
