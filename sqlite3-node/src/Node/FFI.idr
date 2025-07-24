module Node.FFI

import Data.Buffer

export
data JSArray : Type where [external]

-- A JSValue is either:
-- - null
-- - undefined
-- - a string
-- - a number
-- - a BigInt
-- - a Buffer
-- - a Boolean
-- missing:
-- - objects
-- - arrays
export
data JSValue : Type where [external]

export
%foreign """
    node:lambda: (x) => {
      const result = Array();
      while (x.h === undefined) {
        result.push(x.a1); x = x.a2;
      }
      return result;
    }
    """
list2JS : List JSValue -> JSArray

export
str_to_JS : String -> JSValue
str_to_JS x = believe_me x

-- Double is converted to `number`
export
double_to_JS : Double -> JSValue
double_to_JS x = believe_me x

-- int32 is converted to `number`
export
int32_to_JS : Int32 -> JSValue
int32_to_JS x = believe_me x

-- int is converted to `number`
export
int_to_JS : Int -> JSValue
int_to_JS x = believe_me x

-- This relies on the fact that Int64 is converted to BigInt on the JS backend
-- documentation here: https://github.com/idris-lang/Idris2/blob/9cb6c3e40c1fd1ec4447682b4c708ed0df563850/docs/source/backends/javascript.rst#short-example
export
int64_to_JS : Int64 -> JSValue
int64_to_JS x = believe_me x

-- This relies on the fact that Integer is converted to BigInt on the JS backend
-- documentation here: https://github.com/idris-lang/Idris2/blob/9cb6c3e40c1fd1ec4447682b4c708ed0df563850/docs/source/backends/javascript.rst#short-example
export
integer_to_JS : Integer -> JSValue
integer_to_JS x = believe_me x

-- buffers are natively converted to the Buffer type in node
export
buffer_to_JS : Buffer -> JSValue
buffer_to_JS x = believe_me x

export
%foreign "node:lambda: () => true"
JSTrue : JSValue

export
%foreign "node:lambda: () => false"
JSFalse : JSValue

export
bool2JS : Bool -> JSValue
bool2JS True = JSTrue
bool2JS False = JSFalse

export
%foreign "node:lambda: () => undefined"
undefined : JSValue

export
%foreign "node:lambda: () => null"
null : JSValue
