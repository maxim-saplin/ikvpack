import 'dart:async';
import 'dart:isolate';
import 'dart:math';

@TestOn('vm')
import 'package:test/test.dart';
import 'package:ikvpack/ikvpack.dart';

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

class SumAction extends Action {
  final int x;
  final int y;
  SumAction(this.x, this.y);
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
  Future createInstance() async {
    if (faileOnStart) throw 'Failed on start';
    _a = InstanceA();
  }

  @override
  Future receiveRemoteCall(Action action) async {
    switch (action.runtimeType) {
      case SumAction:
        var ac = action as SumAction;
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

late IsolatePool pool;

void main() {
  setUpAll(() async {
    pool = IsolatePool(4);
    await pool.start();
  });

  test('Creating pooled instance', () async {
    var _ = await pool.createInstance(WorkerA(), null);
    expect(true, true); // no exception happens
  });

  test('Creating pooled instance with error', () async {
    var s = '';
    try {
      var _ = await pool.createInstance(WorkerA(true), null);
    } catch (e) {
      s = e.toString();
    }
    expect(s, 'Failed on start');
  });

  test('Simple action returns result', () async {
    var pi = await pool.createInstance(WorkerA());
    var res = await pi.callRemoteMethod(SumAction(2, 2));
    expect(res, 4);
  });

  test('Call callback from pooled instance', () async {
    var completer = Completer<int>();
    var pi = await pool.createInstance(WorkerA(), (a) {
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
    var pi = await pool.createInstance(WorkerA());
    var _ = await pi.callRemoteMethod(CallbackIssuingAction(1));
    //await Future.delayed(Duration(milliseconds: 10000), () => 0);
    // checked manualy debug output to see there's message 'Isolate pool received request to instance 0 which doesnt have callback intialized'
    expect(true, true); // No exceptions
  });

  test('Async action returns result', () async {
    var pi = await pool.createInstance(WorkerA());
    var res = await pi.callRemoteMethod(ConcatAction('Hello ', 'world'));
    expect(res, 'Hello world');
  });

  test('Failed action returns error', () async {
    var s = '';
    try {
      var pi = await pool.createInstance(WorkerA(false));
      await pi.callRemoteMethod(FailAction());
    } catch (e) {
      s = e.toString();
    }
    expect(s, 'Action failed');
  });
}
