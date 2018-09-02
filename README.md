Hash Array Mapped Trie
============

Example
------------

Mutable:

```pascal
var hamt: THAMTStringString;
    p: THAMTStringString.PPair;
begin
  hamt.init;
  hamt.Insert('hello', 'world');
  hamt.insert('foo', 'bar');

  writeln(hamt.get('hello', 'default')); // world
  writeln(hamt.get('foo', 'default')); // bar

  //enumerate all
  for p in hamt do
    writeln(p^.key, ': ', p^.value);

  hamt.release;
end.
```

Immutable using O(1) clone:


```pascal
var hamt1, hamt2: THAMTStringString;
    p: THAMTStringString.PPair;
begin
  hamt1.init;
  hamt1.Insert('hello', 'world');
  hamt2 := hamt1.clone;
  hamt2.insert('foo', 'bar');

  writeln(hamt1.get('hello', 'default')); // world
  writeln(hamt1.get('foo', 'default'));   // default

  hamt1.release;

  writeln(hamt2.get('hello', 'default')); // world
  writeln(hamt2.get('foo', 'default'));   // bar

  hamt2.release;
end.
```