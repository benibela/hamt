Hash Array Mapped Trie (HAMT)
============

A HAMT is a hashmap/set stored as trie, which provides update and lookup performance similarly to a normal hashmap/set, but needs no rehashing and also allows one to copy the entire map/set in constant time.  This implementation uses a 32 bit hash and trie nodes with 32 children, so 5 bits of the hash are consumed to choose the next child. When there are no hash collisions, this HAMT can store 2^32 items with a maximal tree depth of (log_32 2^32) = 6, i.e., you need 6 memory accesses to find any key/value which is practically O(1). Although when there are hash collisions, they are put in an array. 

Each HAMT node carries a reference counter, since FreePascal has no garbage collector. If the reference count is 1, the node can mutate, otherwise it is immutable with a copy-on-write semantic like strings. The counter is updated atomically, so the map could be shared across threads. This might lead to a large number of memory writes when a path of a full tree is copied (6 levels of up 32 children), but still less than copying a full hash table.

Everything is implemented using generics, so it can be used with all types.

Examples
------------

Mutable Map:

```pascal
type TMutableMapStringString = specialize TMutableMap<string, string, THAMTTypeInfo>;
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
type TImmutableMapStringString = specialize TImmutableMap<string, string, THAMTTypeInfo>;
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


* [Maps](https://www.benibela.de/documentation/hamt/hamt.maps.html)

* [Sets](https://www.benibela.de/documentation/hamt/hamt.sets.html)

* [HAMT](https://www.benibela.de/documentation/hamt/hamt.internals.html)

Installation
---------
This library requires no installation, there are no dependencies besides the FreePascal compiler. Just copy the `hamt.*` files in the unit search path of FreePascal.  Then you can use the maps with `uses hamt.maps` and the sets with `uses hamt.sets`.

However, beware that the last stable release of FreePascal, 3.0.4, cannot compile the complex generics used here. You need to have at least FreePascal 3.1.1, preferably 3.3.1 with revision r39690.
 
To run the tests cases in `tests/hamt_tests.pas`, you also need to have [bbutils](https://www.benibela.de/sources_en.html#bbutils) in the search path.
 

 
References
-------
[Ideal Hash Trees](https://infoscience.epfl.ch/record/64398/files/idealhashtrees.pdf)

[Efficient Immutable Collections](https://michael.steindorfer.name/publications/phd-thesis-efficient-immutable-collections.pdf)

