import 'package:test/test.dart';

import '../lib/ikvpack.dart';

void main() {
  test('Spawning isolates works', () async {
    var p = IsolatePool(8);
    await p.start();
    // no exceptions, no test timeout
  }, timeout: Timeout(Duration(seconds: 5)));
}
