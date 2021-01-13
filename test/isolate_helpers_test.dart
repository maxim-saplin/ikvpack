import 'package:ikvpack/ikvpack.dart';
import 'package:ikvpack/src/isolate_helpers.dart';
import 'package:test/test.dart';

class NumbersJob extends PooledJob<int> {
  final int number;

  NumbersJob(this.number);

  @override
  int job() {
    print('Number ${number}');
    return number;
  }
}

class ThrowingNumbersJob extends PooledJob<int> {
  final int number;

  ThrowingNumbersJob(this.number);

  @override
  int job() {
    if (number % 2 == 0) throw 'Error on number ${number}';
    print('Number ${number}');
    return number;
  }
}

class TestSyncCallbackJob extends CallbackIsolateJob<int, int> {
  final int startValue;

  TestSyncCallbackJob(this.startValue) : super(true);

  @override
  Future<int> jobAsync() {
    throw UnimplementedError();
  }

  @override
  int jobSync() {
    var x = startValue;

    if (x == -1) throw 'Simulated error';

    for (var i = 0; i < 10; i++) {
      x++;
      sendDataToCallback(x);
    }

    return x;
  }
}

class TestAsyncCallbackJob extends CallbackIsolateJob<int, int> {
  final int startValue;

  TestAsyncCallbackJob(this.startValue) : super(false);

  @override
  Future<int> jobAsync() async {
    var x = startValue;

    if (x == -1) throw 'Simulated error';

    for (var i = 0; i < 10; i++) {
      await Future.delayed(Duration(milliseconds: 1), () => x++);
      sendDataToCallback(x);
    }

    return x;
  }

  @override
  int jobSync() {
    throw UnimplementedError();
  }
}

void main() {
  group('Isolate pool', () {
    test('Spawning isolates works', () async {
      var p = IsolatePool(8);
      await p.start();
      // no exceptions, no test timeout
    }, timeout: Timeout(Duration(seconds: 5)));

    test('Spawned pool isolates are killed', () async {
      var p = IsolatePool(8);
      await p.start();
      p.stop();
      // no exceptions, no test timeout
    }, timeout: Timeout(Duration(seconds: 5)));

    test('Runing simple job in the pool', () async {
      var p = IsolatePool(4);
      await p.start();

      var futures = <Future>[];
      for (var i = 0; i < 15; i++) {
        futures.add(p.scheduleJob(NumbersJob(101 + i)));
      }

      print(await Future.wait(futures));
    });

    test(
        'Nothing horrible happens if pool is killed while there\'re running jobs',
        () async {
      var p = IsolatePool(4);
      await p.start();

      var futures = <Future>[];
      for (var i = 0; i < 15; i++) {
        futures.add(p.scheduleJob(NumbersJob(101 + i)));
      }
      await futures[0];

      var thrown = false;
      try {
        p.stop();
        print(await Future.wait(futures));
      } catch (e) {
        if (e == 'Isolate pool stopped upon request, canceling jobs') {
          thrown = true;
        }
      }
      expect(thrown, true);
    });

    test('Running pool job that fails is properly handled', () async {
      var p = IsolatePool(3);
      await p.start();

      // ignore: unawaited_futures
      p.scheduleJob(ThrowingNumbersJob(101));
      // ignore: unawaited_futures
      p.scheduleJob(ThrowingNumbersJob(103));
      // ignore: unawaited_futures
      p.scheduleJob(ThrowingNumbersJob(105));
      // ignore: unawaited_futures
      p.scheduleJob(ThrowingNumbersJob(107));

      var thrown = false;
      try {
        await p.scheduleJob(ThrowingNumbersJob(102));
      } catch (_) {
        thrown = true;
      }

      expect(thrown, true);
    });
  });

  group('Callback isolate', () {
    test('Sync job reports progress', () async {
      var called = 0;
      var ci = CallbackIsolate(TestSyncCallbackJob(5));
      var res = await ci.run((arg) => called++);

      expect(called, 10);
      expect(res, 15);
    }, timeout: Timeout(Duration(seconds: 5)));

    test('Sync job re-throws exception', () async {
      var called = 0;
      var ci = CallbackIsolate(TestSyncCallbackJob(-1));

      var thrown = false;
      try {
        var _ = await ci.run((arg) => called++);
      } catch (_) {
        thrown = true;
      }

      expect(thrown, true);
    });

    test('Async job reports progress', () async {
      var called = 0;
      var ci = CallbackIsolate(TestAsyncCallbackJob(10));
      var res = await ci.run((arg) => called++);

      expect(called, 10);
      expect(res, 20);
    }, timeout: Timeout(Duration(seconds: 5)));

    test('Async job re-throws exception', () async {
      var called = 0;
      var ci = CallbackIsolate(TestAsyncCallbackJob(-1));

      var thrown = false;
      try {
        var _ = await ci.run((arg) => called++);
      } catch (_) {
        thrown = true;
      }

      expect(thrown, true);
    });
  });
}