(:~
 : Copyright (C) 2014  Dimitar Popov
 :
 : This program is free software; you can redistribute it and/or modify
 : it under the terms of the GNU General Public License version 2 as
 : published by the Free Software Foundation.
 :
 : This program is distributed in the hope that it will be useful,
 : but WITHOUT ANY WARRANTY; without even the implied warranty of
 : MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 : GNU General Public License for more details.
 :
 : You should have received a copy of the GNU General Public License along
 : with this program; if not, write to the Free Software Foundation, Inc.,
 : 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 :)
declare namespace parallel = "xq-parallel";
declare namespace util = "xq-util";

import module namespace async = 'org.basex.query.AsyncModule';

declare function util:partition($seq as item()*, $n as xs:integer)
as (function() as item()*)* {
  for tumbling window $w in $seq
    start at $s when fn:true()
    end at $e when $e - $s eq $n - 1
  return function() { $w }
};

(: declare function parallel:for-each(
  $partions as item()*,
  $fun as function(item()*) as item()*
) as item()* {
  $partions ! async:eval($fun, .)
}; :)


let (: Test data size :)
    (: $max := 100000, :)
    $max := 100000,
    $cpu := 2,

    $partitions := util:partition((1 to $max), $max idiv $cpu),

    $item-fun := function($i) {
      crypto:hmac(fn:format-integer($i, "0"), "secretkey", "md5", "base64")
    },

    (: for-each implementation: sequential vs parallel :)
    (: $for-each := fn:for-each#2, :)
    $for-each := async:for-each#2,


    $for-each-item-fun := function($p) {
      fn:for-each($p(), $item-fun)
    },
    $for-each-item-fun-eval := function($p) {
      xquery:eval('
        declare variable $p external;
        declare variable $f external;
        $f($p)',
        map {"p": $p, "f": $for-each-item-fun})
    },
    $for-each-item-fun-async := async:eval($for-each-item-fun, ?),
    
    $partition-fun := $for-each-item-fun

return count($for-each($partitions, $partition-fun))

(: (1 to 100000) ! crypto:hmac(fn:format-integer(., "0"), 'secretkey','md5','base64') :)

