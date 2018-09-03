Hash Array Mapped Trie
============

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

  writeln(map.get('hello', 'default')); // world
  writeln(map.get('foo', 'default')); // bar

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

  writeln(map2.get('hello', 'default')); // world
  writeln(map2.get('foo', 'default')); // default

  writeln(map3.get('hello', 'default')); // world
  writeln(map3.get('foo', 'default')); // bar

  //enumerate all
  for p in map3 do
    writeln(p^.key, ': ', p^.value);

  map.free;
  map2.free;
  map3.free;
end.
```