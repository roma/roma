<?php
/**
 * ROMA-Client for PHP5.
 *
 * @version 2009-07-09
 * @author  yosuke.hara@mail.rakuten.co.jp
 */
extension_loaded('romaclient') || dl('romaclient.so');

class RomaClient {
    private static $romaclient = null;

    const SERVER_ERROR = -1;
    const NOT_STORED   = -2;
    const NOT_FOUND    = -3;
    const NOT_CLEARED  = -4;
    const ALIST_NULL   = -5;
    const STORED       =  1;
    const DELETED      =  2;
    const CLEARED      =  3;
    const ALIST_TRUE   =  5;
    const ALIST_FALSE  =  6;
    
    /**
     * constructor.
     * 
     * @param hosts
     */
    private function __construct() {}
    
    /**
     * get roma-client instance.
     *
     * @param hosts (array)
     */
    public static function getInstance($hosts) {
        $hosts_string = "";
        foreach ($hosts as &$value) {        
            $hosts_string .= $value.",";
        }
        $hosts_string =
            substr($hosts_string, 0, strlen($hosts_string)-1);

        //echo $hosts_string, "<br/>";

        $result = rmc_connect($hosts_string);
        if ($result == 1) return False;

        RomaClient::$romaclient = new RomaClient();
        return RomaClient::$romaclient;
    }
    
    /**
     * get value.
     * @param key
     * @return value (string)
     */
    public function get($key) {
        $result = rmc_get($key);
        if ($result == "") {
            $result = NULL;
        }
        return $result;
    }
    
    /**
     * set value.
     * @param key             (string)
     * @param value           (string)
     * @param expire time     (int)
     * @return [success|fail] (bool)
     */
    public function set($key, $value, $exptime) {
        $result = rmc_set($key, $value, $exptime);
        return ($result == RomaClient::STORED ? True : False);
    }
    
    /**
     * add value.
     * @param key             (string)
     * @param value           (string)
     * @param expire time     (int)
     * @return [success|fail] (bool)
     */
    public function add($key, $value, $exptime) {
        $result = rmc_add($key, $value, $exptime);
        return ($result == RomaClient::STORED ? True : False);
    }
    
    /**
     * replace value.
     * @param key             (string)
     * @param value           (string)
     * @param expire time     (int)
     * @return [success|fail] (bool)
     */
    public function replace($key, $value, $exptime) {
        $result = rmc_replace($key, $value, $exptime);
        return ($result == RomaClient::STORED ? True : False);
    }
    
    /**
     * append value.
     * @param key             (string)
     * @param value           (string)
     * @param expire time     (int)
     * @return [success|fail] (bool)
     */
    public function append($key, $value, $exptime) {
        $result = rmc_append($key, $value, $exptime);
        return ($result == RomaClient::STORED ? True : False);
    }
    
    /**
     * prepend value.
     * @param key             (string)
     * @param value           (string)
     * @param expire time     (int)
     * @return [success|fail] (bool)
     */
    public function prepend($key, $value, $exptime) {
        $result = rmc_prepend($key, $value, $exptime);
        return ($result == RomaClient::STORED ? True : False);
    }
    
    /**
     * delete value.
     * @param key             (string)
     * @return [success|fail] (bool)
     */
    public function delete($key) {
        $result = rmc_delete($key);
        return ($result == RomaClient::DELETED ? True : False);
    }
    
    //===== plugin - alist =====//
    /**
     * alist at.
     * @param key   (string)
     * @param index (int)
     * @return value
     */
    public function alist_at($key, $index) {
        $result = rmc_alist_at($key, $index);
        return $result;
    }

    /**
     * alist clear.
     * @param key (string)
     * @return status
     */
    public function alist_clear($key) {
        $result = rmc_alist_clear($key);
        return ($result == RomaClient::CLEARED ? True : False);
    }

    /**
     * alist delete.
     * @param key   (string)
     * @param value (string)
     * @return status
     */
    public function alist_delete($key, $value) {
        $result = rmc_alist_delete($key, $value);
        return ($result == RomaClient::DELETED ? True : False);
    }

    /**
     * alist delete at.
     * @param key   (string)
     * @param index (int)
     * @return status
     */
    public function alist_delete_at($key, $index) {
        $result = rmc_alist_delete_at($key, $index);
        return ($result == RomaClient::DELETED ? True : False);
    }

    /**
     * alist empty ?
     * @param key (string)
     * @return status
     */
    public function alist_empty($key) {
        $result = rmc_alist_empty($key);
        return ($result == RomaClient::ALIST_TRUE ? True : False);
    }

    /**
     * alist first.
     * @param key   (string)
     * @return value
     */
    public function alist_first($key) {
        $result = rmc_alist_first($key);
        return $result;
    }

    /**
     * alist include ?
     * @param key   (string)
     * @param value (string)
     * @return status
     */
    public function alist_include($key, $value) {
        $result = rmc_alist_include($key, $value);
        return ($result == RomaClient::ALIST_TRUE ? True : False);
    }

    /**
     * alist index.
     * @param key   (string)
     * @param value (int)
     * @return index/status
     */
    public function alist_index($key, $value) {
        $result = rmc_alist_index($key, $value);
        return $result;
    }

    /**
     * alist insert.
     * @param key   (string)
     * @param index (int)
     * @param value (string)
     * @return status
     */
    public function alist_insert($key, $index, $value) {
        $result = rmc_alist_insert($key, $index, $value);
        return ($result == RomaClient::STORED ? True : False);
    }

    /**
     * alist sized insert.
     * @param key   (string)
     * @param size  (int)
     * @param value (string)
     * @return status
     */
    public function alist_sized_insert($key, $size, $value) {
        $result = rmc_alist_sized_insert($key, $size, $value);
        return ($result == RomaClient::STORED ? True : False);
    }

    /**
     * alist join.
     * @param key       (string)
     * @param separator (string)
     * @return value    (array)
     */
    public function alist_join($key, $separator) {
        $result = rmc_alist_join($key, $separator);

        if (empty($result))
            return NULL;

        $token = strtok($result, $separator);
        $array = array();
        while($token) {
            array_push($array, ltrim($token, "\r\n"));
            $token = strtok($separator);
        }
        return $array;
    }

    /**
     * alist to json.
     * @param key (string)
     * @return value - json.
     */
    public function alist_to_json($key) {
        $reuslt = rmc_alist_to_json($key);
        return $result;
    }

    /**
     * alist last.
     * @param key (string)
     * @return value
     */
    public function alist_last($key) {
        $result = rmc_alist_last($key);
        return $result;
    }

    /**
     * alist length.
     * @param key (string)
     * @return length/status
     */
    public function alist_length($key) {
        $result = rmc_alist_length($key);
        return $result;
    }

    /**
     * alist pop.
     * @param key (string)
     * @return value
     */
    public function alist_pop($key) {
        $result = rmc_alist_pop($key);
        return $result;
    }

    /**
     * alist push.
     * @param key   (string)
     * @param value (string)
     * @return status
     */
    public function alist_push($key, $value) {
        $result = rmc_alist_push($key, $value);
        return ($result == RomaClient::STORED ? True : False);
    }

    /**
     * alist shift.
     * @param key (string)
     * @return value
     */
    public function alist_shift($key) {
        $result = rmc_alist_shift($key);
        return $result;
    }

    /**
     * alist to string.
     * @param key (string)
     * @return value
     */
    public function alist_to_str($key) {
        $result = rmc_alist_to_str($key);
        return $result;
    }

    /**
     * destructor.
     * 
     */
    public function __destruct() {
        rmc_disconnect();
    }
}
?> 
