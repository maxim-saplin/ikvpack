/// IkvPack abstract class that provides async interface to a certain IkvPack APIs which
/// are mostly sync. There're currently 2 implementations, IkvPackAsyncImpl and IkvPackProxy.
/// The idea is to have no sync methods. This is required to enable communicating with IkvPack
/// proxy class leaving in separate isolate.
/// Factory methods allow creating both variations
/// The former is a wrapper arround IkvPack and is an adapter for sync APIs.
/// The latter creates IkvPackAsync istances in isolates within given isloate pool and
/// communicates with them across isolate boundaries via sort of RPC - this
/// is required for the cases when multiple large IkvPacks need to be quickly created.
/// Simply creating IkvPack in spawned isolate and returning it to main one proved
/// to be very slow due to siginificant overhead when serializing the whole object
abstract class IkvPackAsync {}
