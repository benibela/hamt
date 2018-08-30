{
A collection of often needed functions missing in FPC

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
unit bbhamt;

{$mode objfpc}{$H+}{$ModeSwitch autoderef}{$ModeSwitch advancedrecords}

interface

type
  ppointer = ^pointer;

  THAMTHash = Cardinal;
  THAMTBitmap = record
    function countBeforeIndex(index: THAMTHash): DWord; inline;
    case boolean of
      false: (bits: bitpacked array[0..31] of boolean);
      true: (all: Cardinal);
  end;
  { 4 cases                                           bitmapIsSinglePointer          bitmapIsValue        pointer tag
     - next hamt node (pointer)                             true                        false                 0
     - list (pointer to multiple keys + values)             true                        false                 1
     - empty                                                false                       false                 -
     - one pair (key + value)                               false                       true                  -
  }
  THAMTTaggedPointer = packed record
    raw: pointer;
    function unpack(out isArray: boolean): pointer; inline;
    procedure setToArray(p: pointer); inline;
  end;
  generic THAMTNode<TKey, TValue, TInfo> = packed object
  protected
    type
    PKey = ^TKey;
    PValue = ^TValue;
    TPair = packed record
      key: TKey;
      value: TValue;
    end;
    PPair = ^TPair;
    PHAMTArray = ^THAMTArray;
    THAMTArray = packed object
      refCount: Integer;
      count: integer;
      data: array[0..$FFFFFFFF] of TPair;
      class function size(aCount: integer): SizeInt; static; inline;
      class function allocate(acount: integer): PHAMTArray; static;
      class procedure decrementRefCount(a: PHAMTArray); static;
      procedure incrementChildrenRefCount;
      function indexOf(const key: TKey): integer;
      function find(const key: TKey): PPair;
    end;
    PHAMTNode = ^THAMTNode;
    PPHAMTNode = ^PHAMTNode;

  protected
    refCount: Integer;
    pointerCount, pairCount: word;
    bitmapIsSinglePointer: THAMTBitmap;
    bitmapIsValue: THAMTBitmap;
    pointers: array[0..63] of THAMTTaggedPointer;
    function getPointerOffset(index: THAMTHash): DWord; inline;
    //function getPointer(index: THAMTHash): pointer; inline;
    function getPairOffset(index: THAMTHash): DWord; inline;
    function getPairFromOffset(offset: DWord): PPair; inline;
    function getPairAddr(index: THAMTHash): PPair; inline;
    class procedure hashShift(var hash: THAMTHash; out index: THAMTHash); static; inline;
    class function size(apointerCount, apairCount: integer): SizeInt; static; inline;
    procedure incrementChildrenRefCount;
  public
    class procedure decrementRefCount(node: PHAMTNode); static;
    class function allocate(apointerCount, apairCount: integer): PHAMTNode; static;
  public
    class function insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; allowOverride: boolean): Boolean; static;
    function find(const key:TKey): PPair;
    function contains(const key:TKey):boolean;
    function get(const key: TKey; const def: TValue): TValue;
  end;
  THAMTTypeInfo = object
    class function hash(const s: string): THAMTHash;
    class function equal(const s, t: string): boolean;

    class procedure addRef(var k: string); inline;
    class procedure release(var k: string); inline;
    class procedure assignRef(var target: string; const source: string); inline; //target := source with reference counting
    class procedure assignPtr(var target: string; const source: string); inline;   //assignment without reference counting
  end;
  TInfo = type THAMTTypeInfo;

  generic THAMT<TKey, TValue, TInfo> = packed object
  type THAMTNode = specialize THAMTNode<TKey, TValue, TInfo>;
       PHAMTNode = ^THAMTNode;
  protected
    fcount: SizeInt;
    froot: PHAMTNode;
  public
    property count: SizeInt read fcount;
    procedure init;
    {
    //insert override allowed = true
      result = true                  inserted, no override
      result = false                 inserted, override
    //insert override forbidden = false
      result = true                  inserted, (no override)
      result = false                 not inserted
    }
    function insert(const key: TKey; const value: TValue; allowOverride: boolean = true): boolean;
    function contains(const key:TKey): boolean; inline;
    function get(const key: TKey; const def: TValue): TValue; inline;
    function snapshot: THAMT;
    procedure release;
  end;

  type THAMTStringString = specialize THAMT<string, string, THAMTTypeInfo>;


  function alignedGetMem(s: PtrUInt): pointer; inline;


  const
    BITS_PER_LEVEL = 5;
    LEVEL_HIGH = ( sizeof(THAMTHash) * 8 ) div BITS_PER_LEVEL;
implementation



class function THAMTTypeInfo.hash(const s: string): THAMTHash;
var
  p, last: PByte;
begin
  if s = '' then exit(1);
  p := pbyte(pointer(s));
  last := p + length(s);
  result := 0;

  //testing cases
  case s of
  'test', 'collision+1', 'collision+2', 'collision+3': exit(hash('collision'));
  end;
  if p^ = ord('_') then begin
    inc(p);
    while p < last do begin
      result := (result shl BITS_PER_LEVEL) or ((p^ - ord('0')) * 10 + (((p+1)^ - ord('0') )));
      inc(p, 3);
    end;
    exit;
  end;

  //actual hash
  while p < last do begin
    result := result + p^;
    result := result + (result shl 10);
    result := result xor (result shr 6);
    inc(p);
  end;


  result := result + (result shl 3);
  result := result xor (result shr 11);
  result := result + (result shl 15);
end;

class function THAMTTypeInfo.equal(const s,t:string): boolean;
begin
  result := s = t;
end;

Procedure fpc_AnsiStr_Incr_Ref (S : Pointer); [external name 'FPC_ANSISTR_INCR_REF'];
Procedure fpc_ansistr_decr_ref (Var S : Pointer); [external name 'FPC_ANSISTR_DECR_REF'];


class procedure THAMTTypeInfo.addRef(var k: string);
begin
  fpc_ansistr_incr_ref(pointer(k));
end;

class procedure THAMTTypeInfo.release(var k: string);
begin
  fpc_ansistr_decr_ref(pointer(k));
end;

class procedure THAMTTypeInfo.assignRef(var target: string; const source: string);
begin
  target := source;
end;

class procedure THAMTTypeInfo.assignPtr(var target: string; const source: string);
begin
  pointer(target) := pointer(source);
end;

class procedure THAMTNode.hashShift(var hash: THAMTHash; out index: THAMTHash); inline;
begin
  index := hash and %11111;
  hash := hash shr BITS_PER_LEVEL;
end;

function THAMTBitmap.countBeforeIndex(index: THAMTHash): DWord; inline;
var
  mask: THAMTHash;
begin
  mask := (1 shl index) - 1;
  result := PopCnt(all and mask);
end;

function alignedGetMem(s: PtrUInt): pointer; inline;
begin
  result := GetMem(s);
  while PtrUInt(result) and 1 = 1 do begin
    Freemem(result);
    result := GetMem(s);
  end;
end;

class function THAMTNode.THAMTArray.size(aCount: integer): SizeInt;
begin
  result := 2*sizeof(integer) + sizeof(TPair) * acount
end;

class function THAMTNode.THAMTArray.allocate(acount: integer): PHAMTArray;
begin
  result := alignedGetMem(size(acount)) ;
  result.refCount := 1;
  result.count := acount;
end;

class procedure THAMTNode.THAMTArray.decrementRefCount(a: PHAMTArray);
var
  i: Integer;
begin
  with a^ do begin
    if InterLockedDecrement(refCount) = 0 then begin
      for i := 0 to count - 1 do begin
        TInfo.release(data[i].key);
        TInfo.release(data[i].value);
      end;
      Freemem(a);
    end;
  end;
end;

procedure THAMTNode.THAMTArray.incrementChildrenRefCount;
var
  i: Integer;
begin
  for i := 0 to count - 1 do begin
    TInfo.addRef(data[i].key);
    TInfo.addRef(data[i].value);
  end;
end;

function THAMTNode.THAMTArray.indexOf(const key: TKey): integer;
var
  i: Integer;
begin
  for i := 0 to count - 1 do
    if TInfo.equal(data[i].key, key) then begin
      exit(i);
    end;
  result := -1;
end;

function THAMTNode.THAMTArray.find(const key: TKey): PPair;
var
  index: Integer;
begin
  index := indexOf(key);
  if index >= 0 then result := @data[index]
  else result := nil;
end;

function THAMTTaggedPointer.unpack(out isArray: boolean): pointer; inline;
var
  tag: PtrUInt;
begin
  tag := PtrUInt(raw) and 1;
  isArray := tag <> 0;
  result := pointer(PtrUInt(raw) and not tag);
end;

procedure THAMTTaggedPointer.setToArray(p: pointer);
begin
  raw := pointer(PtrUInt(p) or 1);
end;

function THAMTNode.getPointerOffset(index: THAMTHash): DWord;
begin
  result := bitmapIsSinglePointer.countBeforeIndex(index);
end;

function THAMTNode.getPairOffset(index: THAMTHash): DWord;
begin
  result := bitmapIsValue.countBeforeIndex(index);
end;

function THAMTNode.getPairFromOffset(offset: DWord): PPair;
begin
  result := PPair(@pointers[pointerCount]) + offset;
end;

function THAMTNode.getPairAddr(index: THAMTHash): PPair;
begin
  result := getPairFromOffset(getPairOffset(index));
end;


procedure THAMTNode.incrementChildrenRefCount;
var
  isArray: boolean;
  pointerRaw: Pointer;
  pairs: PPair;
  i: Integer;
begin
  for i := 0 to pointerCount - 1 do begin
    pointerRaw := pointers[i].unpack(isArray);
    if isArray then InterLockedIncrement(PHAMTArray(pointerRaw).refCount)
    else InterLockedIncrement(PHAMTNode(pointerRaw).refCount)
  end;
  pairs := PPair(@pointers[pointerCount]);
  for i := 0 to pairCount - 1 do begin
    tinfo.addRef(pairs[i].key);
    tinfo.addRef(pairs[i].value);
  end;
end;

class procedure THAMTNode.decrementRefCount(node: PHAMTNode);
var
  isArray: boolean;
  pointerRaw: Pointer;
  pairs: PPair;
  i: Integer;
begin
  with node^ do begin
    InterLockedDecrement(refCount);
    if refCount <= 0 then begin
      for i := 0 to pointerCount - 1 do begin
        pointerRaw := pointers[i].unpack(isArray);
        if isArray then THAMTArray.decrementRefCount(PHAMTArray(pointerRaw))
        else decrementRefCount(PHAMTNode(pointerRaw))
      end;
      pairs := PPair(@pointers[pointerCount]);
      for i := 0 to pairCount - 1 do begin
        tinfo.release(pairs[i].key);
        tinfo.release(pairs[i].value);
      end;
      Freemem(node);
    end;
  end;
end;

class function THAMTNode.size(apointerCount, apairCount: integer): SizeInt;
begin
  size := SizeOf(THAMTNode.refCount) + SizeOf(THAMTNode.pointerCount) + SizeOf(THAMTNode.pairCount) + SizeOf(THAMTNode.bitmapIsSinglePointer) + SizeOf(THAMTNode.bitmapIsValue)
                   + SizeOf(Pointer) * apointerCount
                   + SizeOf(TPair) * apairCount;
end;

class function THAMTNode.allocate(apointerCount, apairCount: integer): PHAMTNode;
var
  s: SizeInt;
begin
  s := size(apointerCount, apairCount);
  result := alignedGetMem(s);
  FillChar(result^, s, 0);
  result^.refCount := 1;
  result^.pointerCount := apointerCount;
  result^.pairCount := apairCount;
end;


class function THAMTNode.insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; allowOverride: boolean): Boolean;
var node: PHAMTNode;
  //trigger copy-on-write so the node becomes mutable
  procedure UniqueNode;
  var
    s: SizeInt;
  begin
    if node.refCount > 1 then begin
      {          [ refcount = 2 ]                 =>          [ refcount = 1 ]            [ refcount = 1 ]
       [ refcount = 1 ]        [ refcount = 1 ]                  [ refcount = 2 ]        [ refcount = 2 ]
       }
      node.incrementChildrenRefCount;

      s := size(node.pointerCount, node.pairCount);
      ppnode^ := alignedGetMem(s);
      move(node^, ppnode^^, s);
      decrementRefCount(node);
      ppnode^^.refCount := 1;
      node := ppnode^;
    end;
  end;

var pairIndex: Integer;
  function cloneArray(hamtArrayx: pointer; appendOnePair: Boolean): pointer;
  var HAMTArray: PHAMTArray;
    pair: PPair;
  begin
    HAMTArray := PHAMTArray(hamtArrayx);
    if appendOnePair then begin
      result := THAMTArray.allocate(hamtArray.count + 1);
      pairIndex := hamtArray.count;
      pair := @PHAMTArray(result).data[pairIndex];
      TInfo.assignPtr(pair.key, key);
      TInfo.addRef(pair.key);
      TInfo.assignPtr(pair.value, Default(TValue));
    end else
      result := THAMTArray.allocate(hamtArray.count);
    move(hamtArray.data[0], PHAMTArray(result).data[0], hamtArray.count * sizeof(TPair));
    if hamtArray.refCount > 1 then begin
      hamtArray.incrementChildrenRefCount();
      THAMTArray.decrementRefCount(hamtArray);
    end else
      Freemem(hamtArray);
  end;

var
  i: Integer;
  h, index, h2: THAMTHash;
  offset: DWord;
  procedure movePairsDown(pairsIsArray: boolean; pairs: pointer);
  var node : PHAMTNode;
      dataOffset: integer;
      pair: PPair;
      index2: THAMTHash;
  begin
    while i <= LEVEL_HIGH do begin
      hashShift(h, index);
      hashShift(h2, index2);
      if index = index2 then begin
        //go to next level
        node := THAMTNode.allocate(1, 0);
        node.bitmapIsSinglePointer.all := 1 shl index;
        node.bitmapIsValue.all := 0;
        ppnode^.pointers[offset].raw := node;
        ppnode := @node.pointers[0];
        offset := 0;
      end else begin
        //create node of old pairs and new pair
        if pairsIsArray then begin
          node := THAMTNode.allocate(1, 1);
          node.bitmapIsSinglePointer.all := 1 shl index2;
          node.bitmapIsValue.all := 1 shl index;
          node.pointers[0].setToArray(pairs);
          dataOffset := 0;
        end else begin
          node := THAMTNode.allocate(0, 2);
          node.bitmapIsSinglePointer.all := 0;
          node.bitmapIsValue.all := (1 shl index) or (1 shl index2);
          if index < index2 then dataOffset := 0 else dataOffset := 1;
          pair := node.getPairFromOffset(1 - dataOffset);
          TInfo.assignPtr(pair^.key, PPair(pairs).key);
          TInfo.assignPtr(pair^.value, PPair(pairs).value);
          TInfo.addRef(pair^.key);
          TInfo.addRef(pair^.value);
        end;
        pair := node.getPairFromOffset(dataOffset);
        TInfo.assignRef(pair.key, key);
        TInfo.assignRef(pair.value, value);
        ppnode^.pointers[offset].raw := node;
        exit;
      end;
    end;
    assert(false);
  end;

var
  pairOffset: DWord;
  pair: PPair;
  pointerIsArray: boolean;
  rawPointer: Pointer;
  hamtArray: PHAMTArray;
begin
  result := true;
  node := ppnode^;
  h := TInfo.hash(key);
  //writeln('insert: ', key, ' ', h);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);

    if node.bitmapIsSinglePointer.bits[index] then begin
      UniqueNode;
      offset := node.getPointerOffset(index);
      rawPointer := node.pointers[offset].unpack(pointerIsArray);
      if pointerIsArray then begin
        hamtArray := PHAMTArray(rawPointer);
        h2 := TInfo.hash(hamtArray.data[0].key) shr (BITS_PER_LEVEL * i + BITS_PER_LEVEL);
        if h <> h2 then begin
          //child at index is an array where the keys have a different hash
          //=> move array to a lower level on which the array hash and new key hash end up at a different index
          movePairsDown(true, hamtArray);
        end else begin
          //array and new key have same hash => insert into array
          {Cases:
          key already in array           refCount = 1         => override value
          key already in array           refCount > 1         => clone array, override value in new array
          key not in array               refCount = 1         => resize array: move values from old to new array (do not change ref counts)
          key not in array               refCount > 1         => create larger array, copy values from old to new

          }
          pairIndex := hamtArray.indexOf(key);
          if (pairIndex >= 0) then begin
            result := false;
            if not allowOverride then exit;
          end;
          if (pairIndex < 0) or (hamtArray.refCount > 1) then begin
            hamtArray := cloneArray(hamtArray, pairIndex < 0);
            node.pointers[offset].setToArray(hamtArray);
          end;
          TInfo.assignRef(hamtArray.data[pairIndex].value, value);
        end;
        exit;
      end else begin
        //go to next level
        ppnode := @ppnode^.pointers[offset];
        node := ppnode^;
      end;
    end else if node.bitmapIsValue.bits[index] then begin
      pairOffset := node.getPairOffset(index);
      pair := node.getPairFromOffset(pairOffset);
      if pair.key <> key then begin
        //change pair to array pointer
        if node.refCount > 1 then node.incrementChildrenRefCount;
        offset := node.getPointerOffset(index);
        ppnode^ := allocate(node.pointerCount + 1, node.pairCount - 1);
        //    [ ..head..   ..pointerPrefix..            ..pointerSuffix..    ..pairPrefix..   old pair    ..pairSuffix.. ]
        // -> [ ..head..   ..pointerPrefix.. ..newPointer ..pointerSuffix..  ..pairPrefix..             ..pairSuffix.. ]
        move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * offset); //head, pointer prefix
        move(node.pointers[offset], ppnode^.pointers[offset + 1], sizeof(pointer) * (node.pointerCount - offset) + sizeof(TPair) * pairOffset); //..pointerSuffix..    ..pairPrefix..
        move(node.getPairFromOffset(pairOffset + 1)^ , ppnode^.getPairFromOffset(pairOffset)^, (node.pairCount - pairOffset - 1) * sizeof(TPair) ); //..pairSuffix..
        ppnode^.bitmapIsSinglePointer.bits[index] := true;
        ppnode^.bitmapIsValue.bits[index] := False;
        if i < LEVEL_HIGH then begin
          h2 := TInfo.hash(pair.key) shr (BITS_PER_LEVEL * i + BITS_PER_LEVEL);
          if h <> h2 then begin
            movePairsDown(false, pair);
            decrementRefCount(node);
            exit;
          end;
        end;
        hamtArray := THAMTArray.allocate(2);
        fillchar(hamtArray.data[0], sizeof(TPair) * 2, 0);
        TInfo.assignPtr(hamtArray.data[0].key, pair.key);
        TInfo.assignPtr(hamtArray.data[0].value, pair.value);
        TInfo.assignRef(hamtArray.data[1].key, key);
        TInfo.assignRef(hamtArray.data[1].value, value);
        ppnode^.pointers[offset].setToArray(hamtArray);
        decrementRefCount(node);
      end else begin
        result := false;
        if not allowOverride then exit;
        UniqueNode;
        TInfo.assignRef(node.getPairFromOffset(pairOffset).value, value);
      end;
      exit;
    end else begin
      //copy node and add key+value pair
      offset := node.getPairOffset(index);
      ppnode^ := allocate(node.pointerCount, node.pairCount + 1);
      move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * node.pointerCount + sizeof(TPair) * offset);
      move( node.getPairFromOffset(offset)^ , ppnode^.getPairFromOffset(offset+1)^ , (node.pairCount - offset) * sizeof(TPair) );
      pair := ppnode^.getPairFromOffset(offset);
      TInfo.assignRef(pair.key, key);
      TInfo.assignRef(pair.value, value);
      if node.refCount > 1 then begin
        node.incrementChildrenRefCount;
        decrementRefCount(node)
      end else Freemem(node);
      ppnode^.bitmapIsValue.bits[index] := true;
      exit;
    end;
  end;
end;

function THAMTNode.find(const key: TKey): PPair;
var
  node: PHAMTNode;
  i: Integer;
  h, index: THAMTHash;
  rawPointer: Pointer;
  pointerIsArray: boolean;
begin
  node := @self;
  h := TInfo.hash(key);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    if node.bitmapIsSinglePointer.bits[index] then begin
      rawPointer := node.pointers[node.getPointerOffset(index)].unpack(pointerIsArray);
      if pointerIsArray then
        exit(PHAMTArray(rawPointer).find(key))
       else
        node := PHAMTNode(rawPointer)
    end else if node.bitmapIsValue.bits[index] then begin
      result := node.getPairAddr(index);
      if not TInfo.equal(result.key, key) then result := nil;
      exit;
    end else
      exit(nil);
  end;
  result := nil;
end;

function THAMTNode.contains(const key: TKey): boolean;
begin
  result := find(key) <> nil;
end;

function THAMTNode.get(const key: TKey; const def: TValue): TValue;
var
  pair: PPair;
begin
  pair := find(key);
  if pair = nil then result := def
  else result := pair.value;
end;


procedure THAMT.init;
begin
  froot := THAMTNode.allocate(0,0);
  fcount := 0;
end;

function THAMT.insert(const key: TKey; const value: TValue; allowOverride: boolean): boolean;
begin
  result := THAMTNode.insert(@froot, key, value, allowOverride);
  if Result then Inc(fcount);
end;

function THAMT.contains(const key: TKey): boolean;
begin
  result := froot.contains(key);
end;

function THAMT.get(const key: TKey; const def: TValue): TValue;
begin
  result := froot.get(key, def);
end;

function THAMT.snapshot: THAMT;
begin
  result.fcount := fcount;
  result.froot := froot;
  InterLockedIncrement(froot.refCount);
end;

procedure THAMT.release;
begin
  THAMTNode.decrementRefCount(froot);
end;

end.

