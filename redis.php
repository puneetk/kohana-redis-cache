<?php defined('SYSPATH') or die('No direct script access.');
/**
 * Cache Class for Redis
 */

class Cache_Redis extends Kohana_Cache
{
   protected $_redis = null;
   public function __construct($config) {
       if ( ! extension_loaded('redis')) {
           throw new Cache_Exception(__METHOD__.'Redis PHP extention not loaded');
       }
       parent::__construct($config);
       $this->_redis = new Redis();

       $servers = Arr::get($this->_config, 'servers', null);
       if (empty($servers)) {
           throw new Kohana_Cache_Exception('No Redis servers defined in configuration');
       }
       $method = Arr::get($this->_config,'persistent',false) ? 'pconnect' : 'connect';
       foreach($servers as $server) {
           $this->_redis
               ->{$method}($server['host'], $server['port'], 1);
       }
       // serialize stuff
       $this->_redis
           ->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_IGBINARY);
       // prefix a name space
       $prefix = Arr::get($this->_config, 'prefix');
       if (!empty($prefix)) {
           $this->_redis
               ->setOption(Redis::OPT_PREFIX, $prefix);
       }
       return ;
   }

   // all the functions that in the redis driver are proxied here
   public function __call($method, $args) {
       return call_user_func_array(array($this->_redis, $method), $args);
   }
   public function get($id, $default = null) {
       if (Kohana::$caching === false)
           return null;
       $value = null;
       if (is_array($id)) {
           // sanitize keys
           $ids = array_map(array($this, '_sanitize_id'), $id);
           // return key/value
           $value = array_combine($id, $this->_redis->mget($ids));
       } else {
           // sanitize keys
           $id = $this->_sanitize_id($id);
           $value = $this->_redis->get($id);
       }
       if (empty($value)) {
           $value = $default;
       }
       return $value;
   }
   // supports multi set but assumes count of ids == count of data
   public function set($id, $data, $lifetime = 3600) {
       if (is_array($id)) {
           // sanitize keys
           $ids = array_map(array($this, '_sanitize_id'), $id);
           // use mset to put it all in redis
           $this->_redis->mset(array_combine($ids, array_values($data)));
           $this->_set_ttl($ids, $lifetime);  // give it an array of keys and one lifetime
       } else {
           $id = $this->_sanitize_id($id);
           $this->_redis->mset(array($id=>$data));
           $this->_set_ttl($id, $lifetime);
       }
       return true;
   }
   public function delete($id) {
       $id = $this->_sanitize_id($id);
       return $this->_redis->del($id);
   }
   public function delete_all() {
       return $this->_redis->flushdb();
   }
   protected function _set_ttl($keys, $lifetime = Date::DAY) {
       if (is_int($lifetime))
           $lifetime += time();
       else
           $lifetime = strtotime($lifetime);
       if (is_array($keys)) {
           foreach ($keys as $key) {
               $this->_redis->expireAt($key, $lifetime);
           }
       } else {
           $this->_redis->expireAt($keys, $lifetime);
       }

   }
}
