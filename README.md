Hash Array Mapped Trie (HAMT)
============

A HAMT is a hashmap stored as trie, which provides update and lookup performance similarly to a normal hashmap, but needs no rehashing and also allows one to copy the entire map in constant time.  This implementation uses a 32 bit hash and trie nodes with 32 children, so 5 bits of the hash are consumed to choose the next child. When there are no hash collisions, this HAMT can store 2^32 items with a maximal tree depth of (log_32 2^32) = 6, i.e., you need 6 memory accesses to find any key/value which is practically O(1). (When there are hash collisions, they are put in an array)

Each HAMT node carries a reference counter, since FreePascal has no garbage collector. If the reference count is 1, the node can mutate, otherwise it is immutable with a copy-on-write semantic like strings. The counter is updated atomically, so the map could be shared across threads. 

Everything is implemented using generics, so it can be used with all types.

Examples
------------

Mutable Map:

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

Immutable Map:


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

Mutable Set:
```pascal
type TMutableSetString = specialize TMutableSet<string, THAMTTypeInfo>;
var stringSet: TMutableSetString;
    p: TMutableSetString.PItem;
begin
  stringSet := TMutableSetString.create;
  stringSet.Insert('hello');
  stringSet.insert('foo');

  writeln(stringSet['hello']); // true
  writeln(stringSet.contains('foo')); // true
  writeln(stringSet.contains('abc')); // false

  //enumerate all
  for p in stringSet do
    writeln(p^);

  stringSet.free;
end.
```


Immutable Set:

```pascal
type TImmutableSetString = specialize TImmutableSet<string, THAMTTypeInfo>;
var set1, set2, set3: TImmutableSetString;
  p: TImmutableSetString.PItem;
begin
  set1 := TImmutableSetString.create;
  set2 := set1.Insert('hello');
  set3 := set2.insert('foo');

  writeln(set1.contains('hello')); // false
  writeln(set1['foo']);            // false

  writeln(set2.contains('hello')); // true
  writeln(set2['foo']);            // false

  writeln(set3.contains('hello')); // true
  writeln(set3['foo']);            // true

  //enumerate all
  for p in set3 do
    writeln(p^);

  set1.free;
  set2.free;
  set3.free;
end.
```


Documentation
--------

Manual: 

* [HAMT](https://www.benibela.de/documentation/hamt/bbhamt.html)

* [Maps](https://www.benibela.de/documentation/hamt/bbhamt.maps.html)

* [Sets](https://www.benibela.de/documentation/hamt/bbhamt.sets.html)
 
References
-------
[Ideal Hash Trees](https://infoscience.epfl.ch/record/64398/files/idealhashtrees.pdf)

[Efficient Immutable Collections](https://michael.steindorfer.name/publications/phd-thesis-efficient-immutable-collections.pdf)

