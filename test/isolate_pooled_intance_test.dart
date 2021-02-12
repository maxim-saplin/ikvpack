import 'dart:isolate';

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
}

class SumAction extends Action {
  final int x;
  final int y;
  SumAction(this.x, this.y);
}

class WorkerA extends PooledInstanceWorker {
  late InstanceA _a;
  bool faileOnStart;

  WorkerA([this.faileOnStart = false]);

  @override
  Future createInstance() async {
    _a = InstanceA();
  }

  @override
  Future receiveRemoteCall(Action action) async {
    switch (action.runtimeType) {
      case SumAction:
        var ac = action as SumAction;
        return _a.sum(ac.x, ac.y);
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
}
