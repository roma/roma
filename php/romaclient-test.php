<?php
require "RomaClient.php";

$OK = "OK<br/>";
$NG = "NG<br/>";
$hosts = array('192.168.0.1_11211','192.168.0.1_11212');
$romaclient = RomaClient::getInstance($hosts);
//$romaclient = RomaClient::getInstance("-d,192.168.0.1_12345");

$not_key_1 = $romaclient->get("not_exist_key_001");
echo "not_exist_key_001 <br/>";
$not_key_2 = $romaclient->get("not_exist_key_002");
echo "not_exist_key_002<br/>";
$not_key_3 = $romaclient->get("not_exist_key_003");
echo "not_exist_key_003<br/>";

if (is_null($not_key_1)){
    echo "not_exist_key_001 is null<br/>";
}
if (is_null($not_key_2)){
    echo "not_exist_key_002 is null<br/>";
}
if (is_null($not_key_3)){
    echo "not_exist_key_003 is null<br/>";
}

echo "=== check point 001 === <br/>";

$key01 = "key-test-001";
$key02 = "key-test-002";
$key03 = "key-test-003";
$key04 = "key-test-004";
$key05 = "key-test-005";
$key06 = "key-test-006";
$key07 = "key-test-007";
$key08 = "key-test-008";

$test01 = "value-test-001";
$test02 = "value-test-002 value-test-002";
$test03 = "value-test-003 value-test-003 value-test-003";
$test04 = "value-test-004 value-test-004 value-test-004 value-test-004";
$test05 = "value-test-005 value-test-005 value-test-005 value-test-005 value-test-005";
$test06 = "value-test-006 value-test-006 value-test-006 value-test-006 value-test-006 value-test-006";
$test07 = "value-test-007 value-test-007 value-test-007 value-test-007 value-test-007 value-test-007 value-test-007";
$test08 = "value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008-value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008 value-test-008";

$ret01 = $romaclient->set($key01, $test01, 0);
$ret02 = $romaclient->set($key02, $test02, 0);
$ret03 = $romaclient->set($key03, $test03, 0);
$ret04 = $romaclient->set($key04, $test04, 0);
$ret05 = $romaclient->set($key05, $test05, 0);
$ret06 = $romaclient->set($key06, $test06, 0);
$ret07 = $romaclient->set($key07, $test07, 0);
$ret08 = $romaclient->set($key08, $test08, 0);

$var01 = $romaclient->get($key01);
$var02 = $romaclient->get($key02);
$var03 = $romaclient->get($key03);
$var04 = $romaclient->get($key04);
$var05 = $romaclient->get($key05);
$var06 = $romaclient->get($key06);
$var07 = $romaclient->get($key07);
$var08 = $romaclient->get($key08);

echo "=== check point 002 === <br/>";
echo "<br/><font color='#c0c0c0'>--- set => get ---</font><br/>";

echo $key01, ":", ($test01 == $var01 ? $OK : $NG);
echo $key02, ":", ($test02 == $var02 ? $OK : $NG);
echo $key03, ":", ($test03 == $var03 ? $OK : $NG);
echo $key04, ":", ($test04 == $var04 ? $OK : $NG);
echo $key05, ":", ($test05 == $var05 ? $OK : $NG);
echo $key06, ":", ($test06 == $var06 ? $OK : $NG);
echo $key07, ":", ($test07 == $var07 ? $OK : $NG);
echo $key08, ":", ($test08 == $var08 ? $OK : $NG);
echo "<br/>";
echo $key01, ": [", $ret01, "],", $var01, "<br/>";
echo $key02, ": [", $ret02, "],", $var02, "<br/>";
echo $key03, ": [", $ret03, "],", $var03, "<br/>";
echo $key04, ": [", $ret04, "],", $var04, "<br/>";
echo $key05, ": [", $ret05, "],", $var05, "<br/>";
echo $key06, ": [", $ret06, "],", $var06, "<br/>";
echo $key07, ": [", $ret07, "],", $var07, "<br/>";
echo $key08, ": [", $ret08, "],", $var08, "<br/>";

echo "<br/><font color='#c0c0c0'>--- delete => add ---</font><br/>";
$key11 = "key-test-011";
$test11 = "value-test-011 value-test-011 value-test-011 value-test-011 value-test-011 value-test-011";

$romaclient->delete($key11);
$romaclient->add($key11, $test11, 0);
$var11 = $romaclient->get($key11);

echo $key11, ":", ($test11 == $var11 ? $OK : $NG);
echo $key11, ":", $var11, "<br/>";

echo "<br/><font color='#c0c0c0'>--- append ---</font><br/>";
$romaclient->append($key11, "-append", 0);
$var11 = $romaclient->get($key11);
echo $key11, ":", ($test11+"-append" == $var11 ? $OK : $NG);
echo $key11, ":", $var11, "<br/>";

echo "<br/><font color='#c0c0c0'>--- prepend ---</font><br/>";
$romaclient->prepend($key11, "prepend-", 0);
$var11 = $romaclient->get($key11);
echo $key11, ":", ("prepend-"+$test11 == $var11 ? $OK : $NG);
echo $key11, ":", $var11, "<br/>";

echo "<br/><font color='#c0c0c0'>--- replace ---</font><br/>";
$romaclient->replace($key11, "replace", 0);
$var11 = $romaclient->get($key11);
echo $key11, ":", ("replace" == $var11 ? $OK : $NG);
echo $key11, ":", $var11, "<br/>";


// test alist-basic-commands.
echo "<br/><font color='blue'>=== test alist-basic-commands. ===</font><br/>";
echo "<br/><font color='#c0c0c0'>--- delete => alist_insert ---</font><br/>";
$key21 = "key-test-021";
$romaclient->delete($key21);

$test21_01 = "alist-test-1";
$test21_02 = "alist-test-2";
$test21_03 = "alist-test-3";
$test21_04 = "alist-test-4";
$test21_05 = "alist-test-5";
$test21_06 = "alist-test-6";
$test21_07 = "alist-test-7";
$test21_08 = "alist-test-8";
$test21_09 = "alist-test-9";
$test21_10 = "alist-test-10";
$test21_11 = "alist-test-11";
$test21_12 = "alist-test-12";

$romaclient->alist_insert($key21,  0, $test21_01);
$romaclient->alist_insert($key21,  1, $test21_02);
$romaclient->alist_insert($key21,  2, $test21_03);
$romaclient->alist_insert($key21,  3, $test21_04);
$romaclient->alist_insert($key21,  4, $test21_05);
$romaclient->alist_insert($key21,  5, $test21_06);
$romaclient->alist_insert($key21,  6, $test21_07);
$romaclient->alist_insert($key21,  7, $test21_08);
$romaclient->alist_insert($key21,  8, $test21_09);
$romaclient->alist_insert($key21,  9, $test21_10);
$romaclient->alist_insert($key21, 10, $test21_11);
$romaclient->alist_insert($key21, 11, $test21_12);

$var12 = $romaclient->alist_join($key21, "\t");

if (is_null($var12)) {
    echo $key21, " is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var12); $i++) {
        if ($i == 0)  echo $key21, ":[", $i, "]:", ($var12[0]  == $test21_01 ? $OK : $NG);
        if ($i == 1)  echo $key21, ":[", $i, "]:", ($var12[1]  == $test21_02 ? $OK : $NG);
        if ($i == 2)  echo $key21, ":[", $i, "]:", ($var12[2]  == $test21_03 ? $OK : $NG);
        if ($i == 3)  echo $key21, ":[", $i, "]:", ($var12[3]  == $test21_04 ? $OK : $NG);
        if ($i == 4)  echo $key21, ":[", $i, "]:", ($var12[4]  == $test21_05 ? $OK : $NG);
        if ($i == 5)  echo $key21, ":[", $i, "]:", ($var12[5]  == $test21_06 ? $OK : $NG);
        if ($i == 6)  echo $key21, ":[", $i, "]:", ($var12[6]  == $test21_07 ? $OK : $NG);
        if ($i == 7)  echo $key21, ":[", $i, "]:", ($var12[7]  == $test21_08 ? $OK : $NG);
        if ($i == 8)  echo $key21, ":[", $i, "]:", ($var12[8]  == $test21_09 ? $OK : $NG);
        if ($i == 9)  echo $key21, ":[", $i, "]:", ($var12[9]  == $test21_10 ? $OK : $NG);
        if ($i == 10) echo $key21, ":[", $i, "]:", ($var12[10] == $test21_11 ? $OK : $NG);
        if ($i == 11) echo $key21, ":[", $i, "]:", ($var12[11] == $test21_12 ? $OK : $NG);
    }
}

echo "<br/><font color='#c0c0c0'>--- alist_at ---</font><br/>";
$var22 = $romaclient->alist_at($key21, 0);
echo $key11, ":", ($test21_01 == $var22 ? $OK : $NG);
echo "alist_at[0]: ", $var22, "<br/><br/>";

$var22 = $romaclient->alist_at($key21, 1);
echo $key11, ":", ($test21_02 == $var22 ? $OK : $NG);
echo "alist_at[1]: ", $var22, "<br/><br/>";

$var22 = $romaclient->alist_at($key21, 2);
echo $key11, ":", ($test21_03 == $var22 ? $OK : $NG);
echo "alist_at[2]: ", $var22, "<br/><br/>";

$var22 = $romaclient->alist_at($key21, 3);
echo $key11, ":", ($test21_04 == $var22 ? $OK : $NG);
echo "alist_at[3]: ", $var22, "<br/><br/>";

$var22 = $romaclient->alist_at($key21, 4);
echo $key11, ":", ($test21_05 == $var22 ? $OK : $NG);
echo "alist_at[4]: ", $var22, "<br/><br/>";

echo "<br/><font color='#c0c0c0'>--- alist_delete + alist_delete_at ---</font><br/>";
echo "<br/><font color='#c0c0c0'>--- delete:alist-test-2 ---</font><br/>";
$romaclient->alist_delete($key21, "alist-test-2");
$var13 = $romaclient->alist_join($key21, "\t");
if (is_null($var13)) {
    echo "var13 is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var13); $i++) {
        echo "var13:[", $i, "]:", $var13[$i], "<br/>";
    }
}

echo "<br/><font color='#c0c0c0'>--- delete:alist-test-5 ---</font><br/>";
$romaclient->alist_delete_at($key21, 3);
$var13 = $romaclient->alist_join($key21, "\t");
if (is_null($var13)) {
    echo "var13 is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var13); $i++) {
        echo "var13:[", $i, "]:", $var13[$i], "<br/>";
    }
}

echo "<br/><font color='#c0c0c0'>--- alist_first + alist_last ---</font><br/>";

$var22 = $romaclient->alist_first($key21);
echo "alist first: ", ($test21_01 == $var22 ? $OK : $NG);
echo "alist first: ", $romaclient->alist_first($key21), "<br/><br/>";

$var22 = $romaclient->alist_last($key21);
echo "alist last: ", ($test21_12 == $var22 ? $OK : $NG);
echo "alist last: ", $romaclient->alist_last($key21), "<br/>";


echo "<br/><font color='#c0c0c0'>--- alist_include ---</font><br/>";
echo "include? alist-test-1 : [", $romaclient->alist_include($key21, "alist-test-1"),  "]<br/>";
echo "include? alist-test-4 : [", $romaclient->alist_include($key21, "alist-test-4"),  "]<br/>";
echo "include? alist-test-20: [", $romaclient->alist_include($key21, "alist-test-20"), "]<br/>";


echo "<br/><font color='#c0c0c0'>--- alist_index ---</font><br/>";
echo "index - alist-test-8 : [", $romaclient->alist_index($key21, "alist-test-8"),  "]<br/>";
echo "index - alist-test-10: [", $romaclient->alist_index($key21, "alist-test-10"), "]<br/>";
echo "index - alist-test-20: [", $romaclient->alist_index($key21, "alist-test-20"), "]<br/>";


// test sized insert.
echo "<br/><font color='blue'>=== test sized insert. ===</font><br/>";
$key22 = "test-key-0022";
$test22_01 = "alist-s-test-001-alist-s-test-001-alist-s-test-001-alist-s-test-001-alist-s-test-001-alist-s-test-001";
$test22_02 = "alist-s-test-002-alist-s-test-002-alist-s-test-002-alist-s-test-002-alist-s-test-002-alist-s-test-002";
$test22_03 = "alist-s-test-003-alist-s-test-003-alist-s-test-003-alist-s-test-003-alist-s-test-003-alist-s-test-003";
$test22_04 = "alist-s-test-004-alist-s-test-004-alist-s-test-004-alist-s-test-004-alist-s-test-004-alist-s-test-004";
$test22_05 = "alist-s-test-005-alist-s-test-005-alist-s-test-005-alist-s-test-005-alist-s-test-005-alist-s-test-005";
$test22_06 = "alist-s-test-006-alist-s-test-006-alist-s-test-006-alist-s-test-006-alist-s-test-006-alist-s-test-006";
$test22_07 = "alist-s-test-007-alist-s-test-007-alist-s-test-007-alist-s-test-007-alist-s-test-007-alist-s-test-007";
$test22_08 = "alist-s-test-008-alist-s-test-008-alist-s-test-008-alist-s-test-008-alist-s-test-008-alist-s-test-008";
$test22_09 = "alist-s-test-009-alist-s-test-009-alist-s-test-009-alist-s-test-009-alist-s-test-009-alist-s-test-009";
$test22_10 = "alist-s-test-010-alist-s-test-010-alist-s-test-010-alist-s-test-010-alist-s-test-010-alist-s-test-010";
$test22_11 = "alist-s-test-011-alist-s-test-011-alist-s-test-011-alist-s-test-011-alist-s-test-011-alist-s-test-011";
$test22_12 = "alist-s-test-012-alist-s-test-012-alist-s-test-012-alist-s-test-012-alist-s-test-012-alist-s-test-012";
$test22_13 = "alist-s-test-013-alist-s-test-013-alist-s-test-013-alist-s-test-013-alist-s-test-013-alist-s-test-013";
$test22_14 = "alist-s-test-014-alist-s-test-014-alist-s-test-014-alist-s-test-014-alist-s-test-014-alist-s-test-014";
$test22_15 = "alist-s-test-015-alist-s-test-015-alist-s-test-015-alist-s-test-015-alist-s-test-015-alist-s-test-015";
$test22_16 = "alist-s-test-016-alist-s-test-016-alist-s-test-016-alist-s-test-016-alist-s-test-016-alist-s-test-016";
$test22_17 = "alist-s-test-017-alist-s-test-017-alist-s-test-017-alist-s-test-017-alist-s-test-017-alist-s-test-017";
$test22_18 = "alist-s-test-018-alist-s-test-018-alist-s-test-018-alist-s-test-018-alist-s-test-018-alist-s-test-018";
$test22_19 = "alist-s-test-019-alist-s-test-019-alist-s-test-019-alist-s-test-019-alist-s-test-019-alist-s-test-019";
$test22_20 = "alist-s-test-020-alist-s-test-020-alist-s-test-020-alist-s-test-020-alist-s-test-020-alist-s-test-020";
$test22_21 = "alist-s-test-021-alist-s-test-021-alist-s-test-021-alist-s-test-021-alist-s-test-021-alist-s-test-021";
$test22_22 = "alist-s-test-022-alist-s-test-022-alist-s-test-022-alist-s-test-022-alist-s-test-022-alist-s-test-022";
$test22_23 = "alist-s-test-023-alist-s-test-023-alist-s-test-023-alist-s-test-023-alist-s-test-023-alist-s-test-023";
$test22_24 = "alist-s-test-024-alist-s-test-024-alist-s-test-024-alist-s-test-024-alist-s-test-024-alist-s-test-024";
$test22_25 = "alist-s-test-025-alist-s-test-025-alist-s-test-025-alist-s-test-025-alist-s-test-025-alist-s-test-025";
$test22_26 = "alist-s-test-026-alist-s-test-026-alist-s-test-026-alist-s-test-026-alist-s-test-026-alist-s-test-026";
$test22_27 = "alist-s-test-027-alist-s-test-027-alist-s-test-027-alist-s-test-027-alist-s-test-027-alist-s-test-027";
$test22_28 = "alist-s-test-028-alist-s-test-028-alist-s-test-028-alist-s-test-028-alist-s-test-028-alist-s-test-028";
$test22_29 = "alist-s-test-029-alist-s-test-029-alist-s-test-029-alist-s-test-029-alist-s-test-029-alist-s-test-029";
$test22_30 = "alist-s-test-030-alist-s-test-030-alist-s-test-030-alist-s-test-030-alist-s-test-030-alist-s-test-030";
$test22_31 = "alist-s-test-031-alist-s-test-031-alist-s-test-031-alist-s-test-031-alist-s-test-031-alist-s-test-031";
$test22_32 = "alist-s-test-032-alist-s-test-032-alist-s-test-032-alist-s-test-032-alist-s-test-032-alist-s-test-032";
$test22_33 = "alist-s-test-033-alist-s-test-033-alist-s-test-033-alist-s-test-033-alist-s-test-033-alist-s-test-033";
$test22_34 = "alist-s-test-034-alist-s-test-034-alist-s-test-034-alist-s-test-034-alist-s-test-034-alist-s-test-034";
$test22_35 = "alist-s-test-035-alist-s-test-035-alist-s-test-035-alist-s-test-035-alist-s-test-035-alist-s-test-035";
$test22_36 = "alist-s-test-036-alist-s-test-036-alist-s-test-036-alist-s-test-036-alist-s-test-036-alist-s-test-036";
$test22_37 = "alist-s-test-037-alist-s-test-037-alist-s-test-037-alist-s-test-037-alist-s-test-037-alist-s-test-037";
$test22_38 = "alist-s-test-038-alist-s-test-038-alist-s-test-038-alist-s-test-038-alist-s-test-038-alist-s-test-038";
$test22_39 = "alist-s-test-039-alist-s-test-039-alist-s-test-039-alist-s-test-039-alist-s-test-039-alist-s-test-039";
$test22_40 = "alist-s-test-040-alist-s-test-040-alist-s-test-040-alist-s-test-040-alist-s-test-040-alist-s-test-040";
$test22_41 = "alist-s-test-041-alist-s-test-041-alist-s-test-041-alist-s-test-041-alist-s-test-041-alist-s-test-041";
$test22_42 = "alist-s-test-042-alist-s-test-042-alist-s-test-042-alist-s-test-042-alist-s-test-042-alist-s-test-042";
$test22_43 = "alist-s-test-043-alist-s-test-043-alist-s-test-043-alist-s-test-043-alist-s-test-043-alist-s-test-043";
$test22_44 = "alist-s-test-044-alist-s-test-044-alist-s-test-044-alist-s-test-044-alist-s-test-044-alist-s-test-044";
$test22_45 = "alist-s-test-045-alist-s-test-045-alist-s-test-045-alist-s-test-045-alist-s-test-045-alist-s-test-045";
$test22_46 = "alist-s-test-046-alist-s-test-046-alist-s-test-046-alist-s-test-046-alist-s-test-046-alist-s-test-046";
$test22_47 = "alist-s-test-047-alist-s-test-047-alist-s-test-047-alist-s-test-047-alist-s-test-047-alist-s-test-047";
$test22_48 = "alist-s-test-048-alist-s-test-048-alist-s-test-048-alist-s-test-048-alist-s-test-048-alist-s-test-048";
$test22_49 = "alist-s-test-049-alist-s-test-049-alist-s-test-049-alist-s-test-049-alist-s-test-049-alist-s-test-049";
$test22_50 = "alist-s-test-050-alist-s-test-050-alist-s-test-050-alist-s-test-050-alist-s-test-050-alist-s-test-050";
$test22_51 = "alist-s-test-051-alist-s-test-051-alist-s-test-051-alist-s-test-051-alist-s-test-051-alist-s-test-051";

$romaclient->delete($key22);
echo "sized insert-1 :", ($romaclient->alist_sized_insert($key22, 50, $test22_01) == 1 ? $OK : $NG);
echo "sized insert-2 :", ($romaclient->alist_sized_insert($key22, 50, $test22_02) == 1 ? $OK : $NG);
echo "sized insert-3 :", ($romaclient->alist_sized_insert($key22, 50, $test22_03) == 1 ? $OK : $NG);
echo "sized insert-4 :", ($romaclient->alist_sized_insert($key22, 50, $test22_04) == 1 ? $OK : $NG);
echo "sized insert-5 :", ($romaclient->alist_sized_insert($key22, 50, $test22_05) == 1 ? $OK : $NG);
echo "sized insert-6 :", ($romaclient->alist_sized_insert($key22, 50, $test22_06) == 1 ? $OK : $NG);
echo "sized insert-7 :", ($romaclient->alist_sized_insert($key22, 50, $test22_07) == 1 ? $OK : $NG);
echo "sized insert-8 :", ($romaclient->alist_sized_insert($key22, 50, $test22_08) == 1 ? $OK : $NG);
echo "sized insert-9 :", ($romaclient->alist_sized_insert($key22, 50, $test22_09) == 1 ? $OK : $NG);
echo "sized insert-10:", ($romaclient->alist_sized_insert($key22, 50, $test22_10) == 1 ? $OK : $NG);
echo "sized insert-11:", ($romaclient->alist_sized_insert($key22, 50, $test22_11) == 1 ? $OK : $NG);
echo "sized insert-12:", ($romaclient->alist_sized_insert($key22, 50, $test22_12) == 1 ? $OK : $NG);
echo "sized insert-13:", ($romaclient->alist_sized_insert($key22, 50, $test22_13) == 1 ? $OK : $NG);
echo "sized insert-14:", ($romaclient->alist_sized_insert($key22, 50, $test22_14) == 1 ? $OK : $NG);
echo "sized insert-15:", ($romaclient->alist_sized_insert($key22, 50, $test22_15) == 1 ? $OK : $NG);
echo "sized insert-16:", ($romaclient->alist_sized_insert($key22, 50, $test22_16) == 1 ? $OK : $NG);
echo "sized insert-17:", ($romaclient->alist_sized_insert($key22, 50, $test22_17) == 1 ? $OK : $NG);
echo "sized insert-18:", ($romaclient->alist_sized_insert($key22, 50, $test22_18) == 1 ? $OK : $NG);
echo "sized insert-19:", ($romaclient->alist_sized_insert($key22, 50, $test22_19) == 1 ? $OK : $NG);
echo "sized insert-20:", ($romaclient->alist_sized_insert($key22, 50, $test22_20) == 1 ? $OK : $NG);
echo "sized insert-21:", ($romaclient->alist_sized_insert($key22, 50, $test22_21) == 1 ? $OK : $NG);
echo "sized insert-22:", ($romaclient->alist_sized_insert($key22, 50, $test22_22) == 1 ? $OK : $NG);
echo "sized insert-23:", ($romaclient->alist_sized_insert($key22, 50, $test22_23) == 1 ? $OK : $NG);
echo "sized insert-24:", ($romaclient->alist_sized_insert($key22, 50, $test22_24) == 1 ? $OK : $NG);
echo "sized insert-25:", ($romaclient->alist_sized_insert($key22, 50, $test22_25) == 1 ? $OK : $NG);
echo "sized insert-26:", ($romaclient->alist_sized_insert($key22, 50, $test22_26) == 1 ? $OK : $NG);
echo "sized insert-27:", ($romaclient->alist_sized_insert($key22, 50, $test22_27) == 1 ? $OK : $NG);
echo "sized insert-28:", ($romaclient->alist_sized_insert($key22, 50, $test22_28) == 1 ? $OK : $NG);
echo "sized insert-29:", ($romaclient->alist_sized_insert($key22, 50, $test22_29) == 1 ? $OK : $NG);
echo "sized insert-30:", ($romaclient->alist_sized_insert($key22, 50, $test22_30) == 1 ? $OK : $NG);
echo "sized insert-31:", ($romaclient->alist_sized_insert($key22, 50, $test22_31) == 1 ? $OK : $NG);
echo "sized insert-32:", ($romaclient->alist_sized_insert($key22, 50, $test22_32) == 1 ? $OK : $NG);
echo "sized insert-33:", ($romaclient->alist_sized_insert($key22, 50, $test22_33) == 1 ? $OK : $NG);
echo "sized insert-34:", ($romaclient->alist_sized_insert($key22, 50, $test22_34) == 1 ? $OK : $NG);
echo "sized insert-35:", ($romaclient->alist_sized_insert($key22, 50, $test22_35) == 1 ? $OK : $NG);
echo "sized insert-36:", ($romaclient->alist_sized_insert($key22, 50, $test22_36) == 1 ? $OK : $NG);
echo "sized insert-37:", ($romaclient->alist_sized_insert($key22, 50, $test22_37) == 1 ? $OK : $NG);
echo "sized insert-38:", ($romaclient->alist_sized_insert($key22, 50, $test22_38) == 1 ? $OK : $NG);
echo "sized insert-39:", ($romaclient->alist_sized_insert($key22, 50, $test22_39) == 1 ? $OK : $NG);
echo "sized insert-40:", ($romaclient->alist_sized_insert($key22, 50, $test22_40) == 1 ? $OK : $NG);
echo "sized insert-41:", ($romaclient->alist_sized_insert($key22, 50, $test22_41) == 1 ? $OK : $NG);
echo "sized insert-42:", ($romaclient->alist_sized_insert($key22, 50, $test22_42) == 1 ? $OK : $NG);
echo "sized insert-43:", ($romaclient->alist_sized_insert($key22, 50, $test22_43) == 1 ? $OK : $NG);
echo "sized insert-44:", ($romaclient->alist_sized_insert($key22, 50, $test22_44) == 1 ? $OK : $NG);
echo "sized insert-45:", ($romaclient->alist_sized_insert($key22, 50, $test22_45) == 1 ? $OK : $NG);
echo "sized insert-46:", ($romaclient->alist_sized_insert($key22, 50, $test22_46) == 1 ? $OK : $NG);
echo "sized insert-47:", ($romaclient->alist_sized_insert($key22, 50, $test22_47) == 1 ? $OK : $NG);
echo "sized insert-48:", ($romaclient->alist_sized_insert($key22, 50, $test22_48) == 1 ? $OK : $NG);
echo "sized insert-49:", ($romaclient->alist_sized_insert($key22, 50, $test22_49) == 1 ? $OK : $NG);
echo "sized insert-50:", ($romaclient->alist_sized_insert($key22, 50, $test22_50) == 1 ? $OK : $NG);


echo "<br/><font color='#c0c0c0'>--- check! alist_sized_insert ---</font><br/>";
$var14 = $romaclient->alist_join($key22, "\t");
if (is_null($var14)) {
    echo "var14 is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var14); $i++) {
        echo "var14:[", $i, "]:", $var14[$i], "<br/>";
/*
        if ($i == 0)  echo "[", $i, "]:", ($var14[0]  == $test22_10 ? $OK : $NG);
        if ($i == 1)  echo "[", $i, "]:", ($var14[1]  == $test22_09 ? $OK : $NG);
        if ($i == 2)  echo "[", $i, "]:", ($var14[2]  == $test22_08 ? $OK : $NG);
        if ($i == 3)  echo "[", $i, "]:", ($var14[3]  == $test22_07 ? $OK : $NG);
        if ($i == 4)  echo "[", $i, "]:", ($var14[4]  == $test22_06 ? $OK : $NG);
        if ($i == 5)  echo "[", $i, "]:", ($var14[5]  == $test22_05 ? $OK : $NG);
        if ($i == 6)  echo "[", $i, "]:", ($var14[6]  == $test22_04 ? $OK : $NG);
        if ($i == 7)  echo "[", $i, "]:", ($var14[7]  == $test22_03 ? $OK : $NG);
        if ($i == 8)  echo "[", $i, "]:", ($var14[8]  == $test22_02 ? $OK : $NG);
        if ($i == 9)  echo "[", $i, "]:", ($var14[9]  == $test22_01 ? $OK : $NG);
*/
    }
}

echo "<br/> #=> key22_tostr:[", $romaclient->alist_to_str($key22), "<br/>";

echo "<br/><font color='#c0c0c0'>--- check! alist_sized_insert ---</font><br/>";
echo "sized insert-51 :", ($romaclient->alist_sized_insert($key22, 50, $test22_51) == 1 ? $OK : $NG);

$var14 = $romaclient->alist_join($key22, "\t");
if (is_null($var14)) {
    echo "var14 is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var14); $i++) {
        echo "var14:[", $i, "]:", $var14[$i], "<br/>";
/*
        if ($i == 0)  echo "[", $i, "]:", ($var14[0]  == $test22_11 ? $OK : $NG);
        if ($i == 1)  echo "[", $i, "]:", ($var14[1]  == $test22_10 ? $OK : $NG);
        if ($i == 2)  echo "[", $i, "]:", ($var14[2]  == $test22_09 ? $OK : $NG);
        if ($i == 3)  echo "[", $i, "]:", ($var14[3]  == $test22_08 ? $OK : $NG);
        if ($i == 4)  echo "[", $i, "]:", ($var14[4]  == $test22_07 ? $OK : $NG);
        if ($i == 5)  echo "[", $i, "]:", ($var14[5]  == $test22_06 ? $OK : $NG);
        if ($i == 6)  echo "[", $i, "]:", ($var14[6]  == $test22_05 ? $OK : $NG);
        if ($i == 7)  echo "[", $i, "]:", ($var14[7]  == $test22_04 ? $OK : $NG);
        if ($i == 8)  echo "[", $i, "]:", ($var14[8]  == $test22_03 ? $OK : $NG);
        if ($i == 9)  echo "[", $i, "]:", ($var14[9]  == $test22_02 ? $OK : $NG);
*/
    }
}

// test pop/push/shift.
echo "<br/><font color='blue'>=== test pop/push/shift. ===</font><br/>";
$var_pop = $romaclient->alist_pop($key22);
echo "pop :", $var_pop, "<br/>";
echo "pop :", ($var_pop == $test22_02 ? $OK : $NG);

$var16 = $romaclient->alist_join($key22, "\t");
if (is_null($var16)) {
    echo "var16 is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var16); $i++) {
        echo "var14:[", $i, "]:", $var16[$i], "<br/>";
/*
        if ($i == 0)  echo "[", $i, "]:", ($var14[0]  == $test22_11 ? $OK : $NG);
        if ($i == 1)  echo "[", $i, "]:", ($var14[1]  == $test22_10 ? $OK : $NG);
        if ($i == 2)  echo "[", $i, "]:", ($var14[2]  == $test22_09 ? $OK : $NG);
        if ($i == 3)  echo "[", $i, "]:", ($var14[3]  == $test22_08 ? $OK : $NG);
        if ($i == 4)  echo "[", $i, "]:", ($var14[4]  == $test22_07 ? $OK : $NG);
        if ($i == 5)  echo "[", $i, "]:", ($var14[5]  == $test22_06 ? $OK : $NG);
        if ($i == 6)  echo "[", $i, "]:", ($var14[6]  == $test22_05 ? $OK : $NG);
        if ($i == 7)  echo "[", $i, "]:", ($var14[7]  == $test22_04 ? $OK : $NG);
        if ($i == 8)  echo "[", $i, "]:", ($var14[8]  == $test22_03 ? $OK : $NG);
*/
    }
}

echo "<br/><font color='#c0c0c0'>-----</font><br/>";
$push_val = "test-push-001";
echo "push :", ($romaclient->alist_push($key22, $push_val) == 1 ? $OK : $NG);

$var14 = $romaclient->alist_join($key22, "\t");
if (is_null($var14)) {
    echo "var14 is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var14); $i++) {
        echo "var14:[", $i, "]:", $var14[$i], "<br/>";
/*
        if ($i == 0)  echo "[", $i, "]:", ($var14[0]  == $test22_11 ? $OK : $NG);
        if ($i == 1)  echo "[", $i, "]:", ($var14[1]  == $test22_10 ? $OK : $NG);
        if ($i == 2)  echo "[", $i, "]:", ($var14[2]  == $test22_09 ? $OK : $NG);
        if ($i == 3)  echo "[", $i, "]:", ($var14[3]  == $test22_08 ? $OK : $NG);
        if ($i == 4)  echo "[", $i, "]:", ($var14[4]  == $test22_07 ? $OK : $NG);
        if ($i == 5)  echo "[", $i, "]:", ($var14[5]  == $test22_06 ? $OK : $NG);
        if ($i == 6)  echo "[", $i, "]:", ($var14[6]  == $test22_05 ? $OK : $NG);
        if ($i == 7)  echo "[", $i, "]:", ($var14[7]  == $test22_04 ? $OK : $NG);
        if ($i == 8)  echo "[", $i, "]:", ($var14[8]  == $test22_03 ? $OK : $NG);
        if ($i == 9)  echo "[", $i, "]:", ($var14[9]  == $push_val  ? $OK : $NG);
*/
    }
}

echo "<br/><font color='#c0c0c0'>-----</font><br/>";
//echo "shift :[", $romaclient->alist_shift($key22), "]<br/>";
echo "shift :", ($romaclient->alist_shift($key22) == $test22_51 ? $OK : $NG);

$var14 = $romaclient->alist_join($key22, "\t");
if (is_null($var14)) {
    echo "var14 is NULL.<br/>";
} else {
    for ($i = 0; $i < count($var14); $i++) {
        echo "var14:[", $i, "]:", $var14[$i], "<br/>";
/*
        if ($i == 0)  echo "[", $i, "]:", ($var14[0]  == $test22_10 ? $OK : $NG);
        if ($i == 1)  echo "[", $i, "]:", ($var14[1]  == $test22_09 ? $OK : $NG);
        if ($i == 2)  echo "[", $i, "]:", ($var14[2]  == $test22_08 ? $OK : $NG);
        if ($i == 3)  echo "[", $i, "]:", ($var14[3]  == $test22_07 ? $OK : $NG);
        if ($i == 4)  echo "[", $i, "]:", ($var14[4]  == $test22_06 ? $OK : $NG);
        if ($i == 5)  echo "[", $i, "]:", ($var14[5]  == $test22_05 ? $OK : $NG);
        if ($i == 6)  echo "[", $i, "]:", ($var14[6]  == $test22_04 ? $OK : $NG);
        if ($i == 7)  echo "[", $i, "]:", ($var14[7]  == $test22_03 ? $OK : $NG);
        if ($i == 8)  echo "[", $i, "]:", ($var14[8]  == $push_val  ? $OK : $NG);
*/
    }
}

// test empty/clear.
echo "<br/><font color='blue'>=== test pop/push/shift. ===</font><br/>";
echo "<br/><font color='#c0c0c0'>--- empty? ---</font><br/>";
echo "empty? key21:",($romaclient->alist_empty($key21) != 1 ? $OK : $NG);
echo "empty? key21:[", $romaclient->alist_empty($key21), "]<br/>";

echo "<br/><font color='#c0c0c0'>--- clear => empty ---</font><br/>";
echo "clear  key21:[", $romaclient->alist_clear($key21), "]<br/>";
echo "empty? key21:",($romaclient->alist_empty($key21) == 1 ? $OK : $NG);
echo "empty? key21:[", $romaclient->alist_empty($key21), "]<br/>";

echo "join  :[", $romaclient->alist_join($key21, "\t"),"]<br/>";
$var19 = $romaclient->alist_join($key21, "\t");
if (is_null($var19)) {
    echo "var19 is NULL.<br/>";
    echo $OK;
} else {
    for ($i = 0; $i < count($var19); $i++) {
        echo "var14:[", $i, "]:", $var19[$i], "<br/>";    
    }
}
?>
