Hash Array Mapped Trie (HAMT)
============

A HAMT is a hashmap stored as trie, which provides update and lookup performance similarly to a normal hashmap, but needs no rehashing and also allows one to copy the entire map in constant time.  This implementation uses a 32 bit hash and trie nodes with 32 children, so 5 bits of the hash are consumed to choose the next child. When there are no hash collisions, this HAMT can store 2^32 items with a maximal tree depth of (log_32 2^32) = 6, i.e., you need 6 memory accesses to find any key/value which is practically O(1). (When there are hash collisions, they are put in an array)

Each HAMT node carries a reference counter, since FreePascal has no garbage collector. If the reference count is 1, the node can mutate, otherwise it is immutable with a copy-on-write semantic like strings. The counter is updated atomically, so the map could be shared across threads. 

Everything is implemented using generics, so it can be used with all types.

Examples
------------

Mutable:

```pascal
var map: TMutableMapStringString;
    p: TMutableMapStringString.PPair;
begin
  map := TMutableMapStringString.create;
  map.Insert('hello', 'world');
  map.insert('foo', 'bar');
  map['abc'] := 'def';

  writeln(map['hello']); // world
  writeln(map.get('foo')); // bar
  writeln(map.get('abc', 'default')); // def

  //enumerate all
  for p in map do
    writeln(p^.key, ': ', p^.value);

  map.free;
end.
```

Immutable:


```pascal
var map, map2, map3: TImmutableMapStringString;
    p: TImmutableMapStringString.PPair;
begin
  map := TImmutableMapStringString.create;
  map2 := map.Insert('hello', 'world');
  map3 := map2.insert('foo', 'bar');

  writeln(map.get('hello', 'default')); // default
  writeln(map.get('foo', 'default')); // default

  writeln(map2.get('hello')); // world
  writeln(map2.get('foo', 'default')); // default

  writeln(map3['hello']); // world
  writeln(map3['foo']); // bar

  //enumerate all
  for p in map3 do
    writeln(p^.key, ': ', p^.value);

  map.free;
  map2.free;
  map3.free;
end.
```

Documentation
--------

[Manual](https://www.benibela.de/documentation/hamt/bbhamt.html)
 
References
-------
[Ideal Hash Trees](https://infoscience.epfl.ch/record/64398/files/idealhashtrees.pdf)

[Efficient Immutable Collections](https://michael.steindorfer.name/publications/phd-thesis-efficient-immutable-collections.pdf)

