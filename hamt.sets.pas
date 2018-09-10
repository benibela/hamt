{
Copyright (C) 2018         Benito van der Zander (BeniBela)
                           benito@benibela.de
                           www.benibela.de

This file is distributed under under the same license as Lazarus and the LCL itself:

This file is distributed under the Library GNU General Public License
with the following modification:

As a special exception, the copyright holders of this library give you
permission to link this library with independent modules to produce an
executable, regardless of the license terms of these independent modules,
and to copy and distribute the resulting executable under terms of your choice,
provided that you also meet, for each linked independent module, the terms
and conditions of the license of that module. An independent module is a
module which is not derived from or based on this library. If you modify this
library, you may extend this exception to your version of the library, but
you are not obligated to do so. If you do not wish to do so, delete this
exception statement from your version.

}
{**
@abstract(Mutable and immutable persistent sets as hash array mapped trie (HAMT))

Public generic classes:

* TReadOnlySet

* TMutableSet

* TImmutableSet

Public specialized classes:

* TMutableSetString

* TImmutableSetString


}
unit hamt.sets;

{$mode objfpc}{$H+}{$ModeSwitch autoderef}{$ModeSwitch advancedrecords}

interface

uses
  sysutils, hamt.internals;

type
THAMTTypeInfo = hamt.internals.THAMTTypeInfo;

//** @abstract(Generic read-only set)
//**
//** The data in this set can be read, but there are no public methods to modify it.
generic TReadOnlySet<TItem, TInfo> = class(specialize TReadOnlyCustomSet<TItem, TInfo>)
  type PItem = ^TItem;
  protected
    function forceInclude(const item: TItem; allowOverride: boolean): boolean; inline;
    function forceExclude(const item: TItem): boolean; inline;
  public
    //** Returns if the set contains a certain item
    function contains(const item: TItem): boolean; inline;
    //** Default parameter, so you can test if the set contains an item @code(set[key])
    property items[item: TItem]: Boolean read contains; default;
end;



{** @abstract(Generic mutable set)

Data in this set can be read (see ancestor TReadOnlySet) and modified.

Example:

@longcode(#
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
#)
}
generic TMutableSet<TItem, TInfo> = class(specialize TReadOnlySet<TItem, TInfo>)
private
  procedure toggleItem(item: TItem; AValue: Boolean);
public
  //** Creates an empty set
  constructor Create;
  //** Creates a set equal to other. No data is copied, till either set is modified (copy-on-write).
  constructor Create(other: specialize TReadOnlyCustomSet<TItem, TInfo>);
  //** Inserts an item, if the set does not contain the item or allowOverride is true.
  //** @returns If the set did not contain item.
  function include(const item: TItem; allowOverride: boolean = true): boolean; inline;
  //** Removes an item,
  //** @returns If the set did contain item.
  function exclude(const item: TItem): boolean; inline;
  //** Inserts an item, or raises an exception if the set already contains the item
  procedure insert(const item: TItem); inline;
  //** Removes an item, or raises an exception if the set did not contain the item
  procedure remove(const item: TItem); inline;
  //** Removes everything from the set;
  procedure clear;
  //** Creates a new set equal to self. No data is copied, till either set is modified (copy-on-write).
  function clone: TMutableSet;
  //** Default parameter, so you can test and update the set with @code(set[key])
  property items[item: TItem]: Boolean read contains write toggleItem; default;
end;

{** @abstract(Generic immutable set)

Data in this set can be read (see ancestor TReadOnlySet) and modified by creating new sets.

Example: @longcode(#
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
#)
}
generic TImmutableSet<TItem, TInfo> = class(specialize TReadOnlySet<TItem, TInfo>)
public
  //** Creates an empty set
  constructor Create;
  //** Creates a set equal to other. No data is copied, till either set is modified (copy-on-write).
  constructor Create(other: specialize TReadOnlyCustomSet<TItem, TInfo>);

  //** Creates a new set containing item @code(item). If the set does not contain item or allowOverride is true, item is inserted, otherwise the value is unchanged.
  //** @returns The new set
  function include(const item: TItem; allowOverride: boolean = true): TImmutableSet; inline;
  //** Removes an item
  //** @returns The new set without item
  function exclude(const item: TItem): TImmutableSet; inline;
  //** Inserts an item, or raises an exception if the set already contains the item
  //** @returns The new set
  function insert(const item: TItem): TImmutableSet; inline;
  //** Creates a new set without item, or raises an exception if the set did not contain item
  //** @returns The new set without item
  function remove(const item: TItem): TImmutableSet; inline;

  //** Creates a new set equal to self. No data is copied, till either set is modified (copy-on-write).
  function clone: TImmutableSet;
end;

//** @abstract(A TMutableSet containing strings.)
//** The set handles reference counting and freeing of the strings.
TMutableSetString = specialize TMutableSet<string, THAMTTypeInfo>;
//** @abstract(A TImmutableSet containing strings.)
//** The set handles reference counting and freeing of the strings.
TImmutableSetString = specialize TImmutableSet<string, THAMTTypeInfo>;


implementation


function TReadOnlySet.forceInclude(const item: TItem; allowOverride: boolean): boolean;
begin
  result := THAMTNode.include(@froot, item, allowOverride);
  if Result then Inc(fcount);
end;

function TReadOnlySet.forceExclude(const item: TItem): boolean;
begin
  result := THAMTNode.exclude(@froot, item);
  if result then dec(fcount);
end;


function TReadOnlySet.contains(const item: TItem): boolean;
begin
  result := froot.find(item) <> nil;
end;

{function TReadOnlySet.get(const key: TKey; const def: TValue): TValue;
var
  pair: PPair;
begin
  pair := find(key);
  if pair = nil then result := def
  else result := pair.value;
end;

function TReadOnlySet.get(const key: TKey): TValue;
var
  pair: PPair;
begin
  pair := find(key);
  if pair = nil then raiseMissingKey(key);
  result := pair.value;
end;                  }

procedure TMutableSet.toggleItem(item: TItem; AValue: Boolean);
begin
  if AValue then include(item, true)
  else exclude(item);
end;

constructor TMutableSet.Create;
begin
  froot := THAMTNode.allocateEmpty;
  fcount := 0;
end;

constructor TMutableSet.Create(other: specialize TReadOnlyCustomSet<TItem, TInfo>);
begin
  fcount := other.fcount;
  froot := other.froot;
  InterLockedIncrement(froot.refCount);
end;

function TMutableSet.include(const item: TItem; allowOverride: boolean): boolean;
begin
  result := forceInclude(item, allowOverride);
end;

function TMutableSet.exclude(const item: TItem): boolean;
begin
  result := forceExclude(item);
end;

procedure TMutableSet.insert(const item: TItem);
begin
  if not forceInclude(item, false) then raiseItemError(rsDuplicateItem, item);
end;

procedure TMutableSet.remove(const item: TItem);
begin
  if not forceExclude(item) then raiseItemError(rsMissingItem, item);
end;

procedure TMutableSet.clear;
begin
  THAMTNode.decrementRefCount(froot);
  froot := THAMTNode.allocateEmpty;
  fcount := 0;
end;


function TMutableSet.clone: TMutableSet;
begin
  result := TMutableSet.Create(self);
end;




constructor TImmutableSet.Create;
begin
  froot := THAMTNode.allocateEmpty;
  fcount := 0;
end;

constructor TImmutableSet.Create(other: specialize TReadOnlyCustomSet<TItem, TInfo>);
begin
  fcount := other.fcount;
  froot := other.froot;
  InterLockedIncrement(froot.refCount);
end;

function TImmutableSet.include(const item: TItem; allowOverride: boolean): TImmutableSet;
begin
  result := TImmutableSet.Create(self);
  result.forceInclude(item, allowOverride);
end;

function TImmutableSet.exclude(const item: TItem): TImmutableSet;
begin
  result := TImmutableSet.Create(self);
  result.forceExclude(item);
end;

function TImmutableSet.insert(const item: TItem): TImmutableSet;
begin
  result := TImmutableSet.Create(self);
  if not result.forceInclude(item, false) then begin
    result.free;
    raiseItemError(rsDuplicateItem, item);
  end;
end;

function TImmutableSet.remove(const item: TItem): TImmutableSet;
begin
  result := TImmutableSet.Create(self);
  if not result.forceExclude(item) then begin
    result.free;
    raiseItemError(rsMissingItem, item);
  end;
end;

function TImmutableSet.clone: TImmutableSet;
begin
  result := TImmutableSet.Create(self);
end;


end.

