unit bbhamt;

{$mode objfpc}{$H+}{$ModeSwitch autoderef}{$ModeSwitch advancedrecords}

interface

type {generic THAMT<TKey, TValue, Thash>=class

end;  }
    ppointer = ^pointer;
  TKey = string;
  TValue = string;
  PKey = ^TKey;
  PValue = ^TValue;
  TPair = packed record
    key: TKey;
    value: TValue;
  end;
  PPair = ^TPair;
  TPairArray = array of TPair;

  THAMTHash = Cardinal;
  THAMTBitmap = record
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
  PHAMTNode = ^THAMTNode;
  PPHAMTNode = ^PHAMTNode;
  THAMTTaggedPointer = packed record
    raw: pointer;
    function unpack(out isArray: boolean): pointer; inline;
    procedure setToArray(p: pointer); inline;
  end;
  THAMTNode = packed object
  protected
    type
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
    end;

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
    class function size(apointerCount, apairCount: integer): SizeInt; static; inline;
    procedure incrementChildrenRefCount;
  public
    class procedure decrementRefCount(node: PHAMTNode); static;
    class function allocate(apointerCount, apairCount: integer): PHAMTNode; static;
  public
    class procedure insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; out overriden: boolean); static;
    function contains(const key:TKey):boolean;
    function get(const key: TKey; const def: TValue): TValue;
  end;

  THAMTTypeInfo = object
    class function hash(const s: TKey): THAMTHash;
    class function equal(const s, t: TKey): boolean;

    class procedure addRef(var k: string); inline;
    class procedure release(var k: string); inline;
    class procedure assign(var target: string; const source: string); inline; //target := source
    class procedure move(var target: string; const source: string); inline;   //assignment without reference counting
  end;
  TInfo = type THAMTTypeInfo;


{  THAMTStringString = class

  end;}


implementation

const LEVEL_HIGH = 5;


class function TInfo.hash(const s: TKey): THAMTHash;
var
  p, last: PByte;
begin
  if s = '' then exit(1);
  case s of
  'test', 'collision+1', 'collision+2', 'collision+3': exit(hash('collision'));
  end;
  p := pbyte(pointer(s));
  last := p + length(s);
  result := 0;
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

class function TInfo.equal(const s,t:TKey): boolean;
begin
  result := s = t;
end;

Procedure fpc_AnsiStr_Incr_Ref (S : Pointer); [external name 'FPC_ANSISTR_INCR_REF'];
Procedure fpc_ansistr_decr_ref (Var S : Pointer); [external name 'FPC_ANSISTR_DECR_REF'];


class procedure TInfo.addRef(var k: string);
begin
  fpc_ansistr_incr_ref(pointer(k));
end;

class procedure TInfo.release(var k: string);
begin
  fpc_ansistr_decr_ref(pointer(k));
end;

class procedure TInfo.assign(var target: string; const source: string);
begin
  target := source;
end;

class procedure TInfo.move(var target: string; const source: string);
begin
  pointer(target) := pointer(source);
end;

procedure hashShift(var hash: THAMTHash; out index: THAMTHash); inline;
begin
  index := hash and %11111;
  hash := hash shr 5;
end;

function bitmapCountBeforeIndex(const bitmapAll: Cardinal; index: THAMTHash): DWord; inline;
var
  mask: THAMTHash;
begin
  mask := (1 shl index) - 1;
  result := PopCnt(bitmapAll and mask);
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
  result := bitmapCountBeforeIndex(bitmapIsSinglePointer.all, index);
end;

function THAMTNode.getPairOffset(index: THAMTHash): DWord;
begin
  result := bitmapCountBeforeIndex(bitmapIsValue.all, index);
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


class procedure THAMTNode.insert(ppnode: PPHAMTNode; const key: TKey; const value: TValue; out overriden: boolean);
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
      InterLockedDecrement(node^.refCount);
      ppnode^^.refCount := 1;
      node := ppnode^;
    end;
  end;

var
  i, pairIndex: Integer;
  h, index: THAMTHash;
  offset, pairOffset: DWord;
  pair: PPair;
  pointerIsArray: boolean;
  rawPointer: Pointer;
  hamtArray, hamtArrayNew: PHAMTArray;
begin
  overriden := false;
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
        //insert into array
        {Cases:
        key already in array           refCount = 1         => override value
        key already in array           refCount > 1         => clone array, override value in new array
        key not in array               refCount = 1         => resize array: move values from old to new array (do not change ref counts)
        key not in array               refCount > 1         => create larger array, copy values from old to new

        }
        hamtArray := PHAMTArray(rawPointer);
        pairIndex := hamtArray.indexOf(key);
        overriden := pairIndex >= 0;
        if (pairIndex < 0) or (hamtArray.refCount > 1) then begin
          if pairIndex < 0 then begin
            hamtArrayNew := THAMTArray.allocate(hamtArray.count + 1);
            pairIndex := hamtArray.count;
            TInfo.Move(hamtArrayNew.data[pairIndex].key, key);
            TInfo.addRef(hamtArrayNew.data[pairIndex].key);
            TInfo.move(hamtArrayNew.data[pairIndex].value, Default(TValue));
          end else hamtArrayNew := THAMTArray.allocate(hamtArray.count);
          move(hamtArray.data[0], hamtArrayNew.data[0], hamtArray.count * sizeof(TPair));
          if hamtArray.refCount > 1 then begin
            hamtArray.incrementChildrenRefCount();
            THAMTArray.decrementRefCount(hamtArray);
          end else Freemem(hamtArray);
          hamtArray := hamtArrayNew;
          node.pointers[offset].setToArray(hamtArray);
        end;
        TInfo.assign(hamtArray.data[pairIndex].value, value);
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

        hamtArray := THAMTArray.allocate(2);
        fillchar(hamtArray.data[0], sizeof(TPair) * 2, 0);
        TInfo.move(hamtArray.data[0].key, pair.key);
        TInfo.move(hamtArray.data[0].value, pair.value);
        TInfo.assign(hamtArray.data[1].key, key);
        TInfo.assign(hamtArray.data[1].value, value);
        ppnode^.pointers[offset].setToArray(hamtArray);
        decrementRefCount(node);
        ppnode^.bitmapIsSinglePointer.bits[index] := true;
        ppnode^.bitmapIsValue.bits[index] := False;
      end else TInfo.assign(pair.value, value);
      exit;
    end else begin
      //add key+value pair to node
      if node.refCount > 1 then node.incrementChildrenRefCount;
      offset := node.getPairOffset(index);
      ppnode^ := allocate(node.pointerCount, node.pairCount + 1);
      move(node.bitmapIsSinglePointer, ppnode^.bitmapIsSinglePointer, sizeof(THAMTBitmap) + sizeof(THAMTBitmap) + sizeof(Pointer) * node.pointerCount + sizeof(TPair) * offset);
      move( node.getPairFromOffset(offset)^ , ppnode^.getPairFromOffset(offset+1)^ , (node.pairCount - offset) * sizeof(TPair) );
      pair := ppnode^.getPairFromOffset(offset);
      TInfo.assign(pair.key, key);
      TInfo.assign(pair.value, value);
      decrementRefCount(node);
      ppnode^.bitmapIsValue.bits[index] := true;
      exit;
    end;
  end;
end;

function THAMTNode.contains(const key: TKey): boolean;
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
      rawPointer := node.pointers[getPointerOffset(index)].unpack(pointerIsArray);
      if pointerIsArray then
        exit(PHAMTArray(rawPointer).indexOf(key) >= 0)
       else
        node := PHAMTNode(rawPointer)
    end else if node.bitmapIsValue.bits[index] then begin
      exit(TInfo.equal(node.getPairAddr(index).key, key));
    end else
      exit(false);
  end;
  result := false;
end;

function THAMTNode.get(const key: TKey; const def: TValue): TValue;
var
  node: PHAMTNode;
  i, arrayIndex: Integer;
  h, index: THAMTHash;
  pair: PPair;
  rawPointer: Pointer;
  pointerIsArray: boolean;
begin
  node := @self;
  h := TInfo.hash(key);
  for i := 0 to LEVEL_HIGH do begin
    hashShift(h, index);
    if node.bitmapIsSinglePointer.bits[index] then begin
      rawPointer := node.pointers[getPointerOffset(index)].unpack(pointerIsArray);
      if pointerIsArray then begin
        arrayIndex := PHAMTArray(rawPointer).indexOf(key);
        if arrayIndex >= 0 then result := PHAMTArray(rawPointer).data[arrayIndex].value
        else result := def;
        exit;
      end else begin
        node := PHAMTNode(rawPointer)
      end;
    end else if node.bitmapIsValue.bits[index] then begin
      pair := node.getPairAddr(index);
      if TInfo.equal(pair.key, key) then result := pair.value
      else result := def;
      exit
    end else
      exit(def);
  end;
  result := def;
end;

end.

