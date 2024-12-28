@JS()
library;

import 'dart:js';
import 'dart:typed_data';
import 'package:js/js.dart';
import 'package:js/js_util.dart';

bool _bulkInsertInitialized = false;

const String _bulkInsert =
    'function bulkInsert(e,t,n,o,r){return o||(o=0),r||(r=t.length-1),new Promise((c,s)=>{const u=e.transaction(["keys"],"readwrite");u.oncomplete=function(){c(void 0)},u.onerror=function(e){s(e.target.error)};const a=e.transaction(["values"],"readwrite");a.oncomplete=function(){c(void 0)},a.onerror=function(e){s(e.target.error)};const i=u.objectStore("keys"),f=a.objectStore("values");for(var l=o;l<=r;l++){f.add(n[l].buffer,l);const e=i.add(t[l],l);l==t.length-1&&(e.onsuccess=function(){kResult=e.result})}})}';

Future<void> insert(dynamic db, List<String> keys, List<Uint8List> values,
    [int startIndex = -1, int endIndex = -1]) {
  // print(startIndex);
  // print(endIndex);
  assert((startIndex < 0 && endIndex < 0) || (startIndex > -1 && endIndex > -1),
      'startIndex and endIndex must be both either set or left unchanged');
  assert(startIndex < keys.length, 'startIndex invalid');
  assert(endIndex < keys.length, 'endIndex invalid');
  assert(
      endIndex >= startIndex, 'endIndex must be grater or equal to startIndex');
  if (!_bulkInsertInitialized) {
    context.callMethod('eval', [_bulkInsert]);
    _bulkInsertInitialized = true;
  }

  if (startIndex < 0) startIndex = 0;
  if (endIndex < 0) endIndex = keys.length - 1;

  var promise = bulkInsert(db, keys, values, startIndex, endIndex);

  return promiseToFuture(promise);
}

@JS()
external Object bulkInsert(dynamic db, List<String> keys,
    List<Uint8List> values, int startIndex, int endIndex);

Future<void> toFuture(Object promise) {
  return promiseToFuture(promise);
}

// function bulkInsert(db, keys, values, startIndex, endIndex) {
//   if (!startIndex) startIndex = 0;
//   if (!endIndex) endIndex = keys.length-1;
//   return new Promise((resolve, reject) => {
//     let kKesult;
//     const txKeys = db.transaction(["keys"], "readwrite");
//     txKeys.oncomplete = function() {
//       resolve(kKesult);
//     };
//     txKeys.onerror = function(event) {
//       reject(event.target.error);
//     }
//     let vResult;
//     const txValues = db.transaction(["values"], "readwrite");
//     txValues.oncomplete = function() {
//       resolve(vResult);
//     };
//     txValues.onerror = function(event) {
//       reject(event.target.error);
//     }
//     const k = txKeys.objectStore("keys");
//     const v = txValues.objectStore("values");

//     for (var i = startIndex; i <= endIndex; i++)
//     {
//       v.add(values[i].buffer, i);
//       const request = k.add(keys[i], i); 
//       if (i == keys.length-1) {
//         request.onsuccess = function() {
//            kResult = request.result;
//         }
//       }
//     }
//   });
// }