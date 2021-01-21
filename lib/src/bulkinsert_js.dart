@JS()
library bulkinsert;

import 'dart:js';
import 'dart:typed_data';
import 'package:js/js.dart';
import 'package:js/js_util.dart';

bool _bulkInsertInitialized = false;

const String _bulkInsert =
    'function bulkInsert(o,n,t){return new Promise((e,r)=>{let c;const s=o.transaction(["ikv"],"readwrite");s.oncomplete=function(){e(c)},s.onerror=function(o){r(o.target.error)};const u=s.objectStore("ikv");for(var l=0;l<n.length;l++){const o=u.put(t[l].buffer,n[l]);l==n.length-1&&(o.onsuccess=function(){c=o.result})}})}function testArrays(o,n){console.log(o),console.log(n)}';

Future<void> insert(dynamic db, List<String> keys, List<Uint8List> values) {
  if (!_bulkInsertInitialized) {
    context.callMethod('eval', [_bulkInsert]);
    _bulkInsertInitialized = true;
  }

  var promise = bulkInsert(db, keys, values);

  return promiseToFuture(promise);
}

@JS()
external Object bulkInsert(
    dynamic db, List<String> keys, List<Uint8List> values);

Future<void> toFuture(Object promise) {
  return promiseToFuture(promise);
}

// function bulkInsert(db, keys, values) {
//   return new Promise((resolve, reject) => {
//     let result;
//     const tx = db.transaction(["ikv"], "readwrite");
//     tx.oncomplete = function() {
//       resolve(result);
//     };
//     tx.onerror = function(event) {
//       reject(event.target.error);
//     }
//     const store = tx.objectStore("ikv");
//     for (var i = 0; i < keys.length; i++)
//     {
//       const request = store.put(values[i].buffer, keys[i]);
//       if (i == keys.length-1) {
//         request.onsuccess = function() {
//            result = request.result;
//         }
//       }
//     }

//   });
// }

// function testArrays(keys, values) {
//   console.log(keys);
//   console.log(values);
// }