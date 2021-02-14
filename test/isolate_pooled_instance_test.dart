@TestOn('vm')

import 'package:test/test.dart';
import 'package:ikvpack/ikvpack.dart';
import 'dart:async';

class InstanceA {
  int sum(int x, int y) {
    return x + y;
  }

  Future wait(int durationMs) async {
    await Future.delayed(Duration(milliseconds: durationMs));
  }

  Future<String> concat(String arg1, String arg2) {
    return Future(() => arg1 + arg2);
  }

  void deffered(int value, Function callback) {
    Future.delayed(Duration(milliseconds: 10), () {
      callback(value + 1);
    });
  }

  void fail() {
    throw 'Action failed';
  }
}

class InstanceB {
  double sum(double x, double y) {
    return x + y;
  }
}

class SumIntAction extends Action {
  final int x;
  final int y;
  SumIntAction(this.x, this.y);
}

class SumDynamicAction extends Action {
  final dynamic x;
  final dynamic y;
  SumDynamicAction(this.x, this.y);
}

class ConcatAction extends Action {
  final String x;
  final String y;
  ConcatAction(this.x, this.y);
}

class CallbackIssuingAction extends Action {
  final int x;
  CallbackIssuingAction(this.x);
}

class CallbackAction extends Action {
  final int x;
  CallbackAction(this.x);
}

class FailAction extends Action {}

class WorkerA extends PooledInstanceWorker {
  late InstanceA _a;
  bool faileOnStart;

  WorkerA([this.faileOnStart = false]);

  @override
  Future init() async {
    if (faileOnStart) throw 'Failed on start';
    _a = InstanceA();
  }

  @override
  Future receiveRemoteCall(Action action) async {
    switch (action.runtimeType) {
      case SumIntAction:
        var ac = action as SumIntAction;
        return _a.sum(ac.x, ac.y);
      case ConcatAction:
        var ac = action as ConcatAction;
        return _a.concat(ac.x, ac.y);
      case CallbackIssuingAction:
        var ac = action as CallbackIssuingAction;
        return _a.deffered(ac.x, (y) async {
          var x = await callRemoteMethod<int>(CallbackAction(y));
          await callRemoteMethod(CallbackAction(x + 1));
        });
      case FailAction:
        return _a.fail();
    }
  }
}

class WorkerB extends PooledInstanceWorker {
  late InstanceA _a;
  late InstanceB _b;

  @override
  Future init() async {
    _a = InstanceA();
    _b = InstanceB();
  }

  @override
  Future receiveRemoteCall(Action action) async {
    switch (action.runtimeType) {
      case SumIntAction:
        var ac = action as SumIntAction;
        return _a.sum(ac.x, ac.y);
      case SumDynamicAction:
        var ac = action as SumDynamicAction;
        if (ac.x is int) return _a.sum(ac.x, ac.y);
        if (ac.x is double) return _b.sum(ac.x, ac.y);
        throw 'SumDynamic supports only int and double';
      default:
        throw 'Unknown action recevied';
    }
  }
}

late IsolatePool gPool;
late PooledInstance pia;
late PooledInstance pib;

void main() {
  test('Creating pooled instance succeeds', () async {
    var _pool = IsolatePool(4);
    await _pool.start();
    var _ = await _pool.createInstance(WorkerA(), null);
    expect(_pool.numberOfPooledInstances, 1);
  });

  test('Creating multiple pooled instance succeeds', () async {
    var _pool = IsolatePool(4);
    await _pool.start();
    for (var i = 0; i < 20; i++) {
      var _ = await _pool.createInstance(WorkerA(), null);
    }
    expect(_pool.numberOfPooledInstances, 20);
  });

  test('Creating different type pooled instance succeeds', () async {
    var _pool = IsolatePool(4);
    await _pool.start();
    for (var i = 0; i < 20; i++) {
      var _ =
          await _pool.createInstance(i % 2 == 0 ? WorkerA() : WorkerB(), null);
    }
    expect(_pool.numberOfPooledInstances, 20);
  });

  test('Instances are created in different isolates', () async {
    var _pool = IsolatePool(4);
    await _pool.start();
    var instances = <PooledInstance>[];
    for (var i = 0; i < 20; i++) {
      var pi =
          await _pool.createInstance(i % 2 == 0 ? WorkerA() : WorkerB(), null);
      instances.add(pi);
    }
    expect(_pool.numberOfPooledInstances, 20);

    var pools = List<int>.filled(4, 0);

    for (var pi in instances) {
      pools[_pool.indexOfPi(pi)]++;
    }

    for (var p in pools) {
      expect(p, 5);
    }
  });

  test('Can destroy pooled instances', () async {
    var _pool = IsolatePool(4);
    await _pool.start();
    var instances = <PooledInstance>[];
    for (var i = 0; i < 5; i++) {
      var pi = await _pool.createInstance(WorkerA(), null);
      instances.add(pi);
    }
    expect(_pool.numberOfPooledInstances, 5);

    _pool.destroyInstance(instances[0]);
    expect(_pool.numberOfPooledInstances, 4);

    var err = '';

    try {
      _pool.destroyInstance(instances[0]);
    } catch (e) {
      err = e.toString();
    }

    expect(err != '', true);
  });

  test('Can stop while there\'re instances being created', () async {
    var _pool = IsolatePool(5);
    await _pool.start();

    late Future f;
    var err = '';

    try {
      for (var i = 0; i < 25; i++) {
        f = _pool.createInstance(WorkerA(), null);
        if (i < 24) await f;
      }
      //expect(_pool.numberOfPooledInstances, 0);

      _pool.stop();
      await f;
    } catch (e) {
      err = e.toString();
    }

    expect(err,
        'Isolate pool stopped upon request, cancelling instance creation requests');
  });

  test('Can stop while there\'re requests pending', () async {
    var _pool = IsolatePool(5);
    await _pool.start();

    late Future f;
    late PooledInstance pi;
    var err = '';

    try {
      for (var i = 0; i < 25; i++) {
        pi = await _pool.createInstance(WorkerA(), null);
      }

      expect(_pool.numberOfPooledInstances, 25);

      for (var i = 0; i < 25; i++) {
        f = pi.callRemoteMethod<int>(SumIntAction(i, 1));
        if (i < 24) await f;
      }

      _pool.stop();
      await f;
    } catch (e) {
      err = e.toString();
    }

    expect(
        err, 'Isolate pool stopped upon request, cancelling pending request');
  });

  test('Creating pooled instance with error', () async {
    var _pool = IsolatePool(4);
    await _pool.start();
    var s = '';
    try {
      var _ = await _pool.createInstance(WorkerA(true), null);
    } catch (e) {
      s = e.toString();
    }
    expect(s, 'Failed on start');
  });

  group('WorkerA', () {
    setUpAll(() async {
      gPool = IsolatePool(4);
      await gPool.start();
      pia = await gPool.createInstance(WorkerA());
    });

    tearDownAll(() {
      gPool.stop();
    });

    test('Simple action returns result', () async {
      var res = await pia.callRemoteMethod(SumIntAction(2, 2));
      expect(res, 4);
    });

    test('Call callback from pooled instance', () async {
      var completer = Completer<int>();
      var pi = await gPool.createInstance(WorkerA(), (a) {
        completer.complete((a as CallbackAction).x);
        return a.x + 1;
      });
      var _ = await pi.callRemoteMethod(CallbackIssuingAction(1));
      var res = await completer.future;
      expect(res, 2);
      // the callback will be called twice from isolate, each time adding 1 to whatever it receives
      completer = Completer<int>();
      res = await completer.future;
      expect(res, 4);
    });

    test('Call null callback from pooled instance', () async {
      var _ = await pia.callRemoteMethod(CallbackIssuingAction(1));
      //await Future.delayed(Duration(milliseconds: 10000), () => 0);
      // checked manualy debug output to see there's message 'Isolate pool received request to instance 0 which doesnt have callback intialized'
      expect(true, true); // No exceptions
    });

    test('Async action returns result', () async {
      var res = await pia.callRemoteMethod(ConcatAction('Hello ', 'world'));
      expect(res, 'Hello world');
    });

    test('Reqeusts number grows and declines', () async {
      var r1 = pia.callRemoteMethod(ConcatAction('Hello ', 'world'));
      expect(gPool.numberOfPendingRequests, 1);
      var r2 = pia.callRemoteMethod(ConcatAction('Hello ', 'world'));
      expect(gPool.numberOfPendingRequests, 2);
      await Future.wait([r1, r2]);
      expect(gPool.numberOfPendingRequests, 0);
    });

    test('Failed action returns error', () async {
      var s = '';
      try {
        await pia.callRemoteMethod(FailAction());
      } catch (e) {
        s = e.toString();
      }
      expect(s, 'Action failed');
    });

    test('Sending second request before the first completes', () async {
      var f1 = pia.callRemoteMethod(CallbackIssuingAction(1));
      var f2 = pia.callRemoteMethod(SumIntAction(1, 1));

      var _ = await f1;
      var x2 = await f2;
      expect(x2, 1.0 + 1.0);
    });
  });

  group('WorkerB', () {
    setUpAll(() async {
      gPool = IsolatePool(4);
      await gPool.start();
      pia = await gPool.createInstance(WorkerA());
      pib = await gPool.createInstance(WorkerB());
    });

    tearDownAll(() {
      gPool.stop();
    });

    test('Simple action returns result', () async {
      var i = await pib.callRemoteMethod<int>(SumIntAction(2, 2));
      expect(i, 4);
      i = await pib.callRemoteMethod<int>(SumDynamicAction(1, 2));
      expect(i, 3);
      var d = await pib.callRemoteMethod<double>(SumDynamicAction(10.5, 10.5));
      expect(d, 10.5 + 10.5);
    });

    test('Calling unknown action when wroker is supposed to throw', () async {
      var s = '';
      try {
        await pib.callRemoteMethod(ConcatAction('', ''));
      } catch (e) {
        s = e.toString();
      }
      expect(s, 'Unknown action recevied');
    });

    test(
        'Calling action with unexpected params when wroker is supposed to throw',
        () async {
      var s = '';
      try {
        var _ = await pib.callRemoteMethod<int>(SumDynamicAction('', ''));
      } catch (e) {
        s = e.toString();
      }
      expect(s, 'SumDynamic supports only int and double');
    });

    test('Sending second request before the first completes', () async {
      var f1 = pib.callRemoteMethod(SumIntAction(1, 1));
      var f2 = pib.callRemoteMethod(SumDynamicAction(1.0, 1.0));
      var x1 = await f1;
      var x2 = await f2;
      expect(x1, 2);
      expect(x2, 1.0 + 1.0);
    });
  });
}
