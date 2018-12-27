package com.jut.redis;

import redis.clients.jedis.ListPosition;

import java.util.List;
import java.util.Map;
import java.util.Set;

/***
 * redis管理接口
 * 具体的方法请参考redis命令
 * @author geodon
 * @date 20181227
 */
public interface RedisManager {
    public String get(String key);
    public String set(String key,String value);
    public String setex(String key,int seconds,String value);
    public Long setrange(String key,int offset,String value);
    public List<String> mget(String...keys);
    public String mset(String...keysvalues);
    public Long append(String key,String value);
    public Boolean exists(String key);
    public Long setnx(String key,String value);
    public Long expire(String key,int seconds);
    public Long del(String...keys);
    public Long timetolive(String key);
    public Long msetnx(String...keysvalues);
    public String getset(String key,String value);
    public String getrange(String key, int startOffset ,int endOffset);
    public Long incr(String key);
    public Long incrBy(String key,Long integer);
    public Long decr(String key);
    public Long decrBy(String key,Long integer);
    public Long serlen(String key);
    public Long hset(String key,String field,String value);
    public Long hsetnx(String key,String field,String value);
    public String hmset(String key, Map<String, String> hash);
    public String hget(String key, String field);
    public List<String> hmget(String key,String...fields);
    public Long hincrby(String key ,String field ,Long value);
    public Boolean hexists(String key , String field);
    public Long hlen(String key);
    public Long hdel(String key ,String...fields);
    public Set<String> hkeys(String key);
    public List<String> hvals(String key);
    public Map<String, String> hgetall(String key);
    public Long lpush(String key ,String...strs);
    public Long rpush(String key ,String...strs);
    public Long linsert(String key, ListPosition where,String pivot, String value);
    public String lset(String key ,Long index, String value);
    public Long lrem(String key,long count,String value);
    public String ltrim(String key ,long start ,long end);
    public String lpop(String key);
    public String rpop(String key);
    public String rpoplpush(String srckey, String dstkey);
    public String lindex(String key,long index);
    public Long llen(String key);
    public List<String> lrange(String key, long start, long end);
    public Long sadd(String key,String...members);
    public Long srem(String key,String...members);
    public String spop(String key);
    public Set<String> sdiff(String...keys);
    public Long sdiffstore(String dstkey,String... keys);
    public Set<String> sinter(String...keys);
    public Long sinterstore(String dstkey,String...keys);
    public Set<String> sunion(String... keys);
    public Long sunionstore(String dstkey,String...keys);
    public Long smove(String srckey, String dstkey, String member);
    public Long scard(String key);
    public Boolean sismember(String key,String member);
    public String srandmember(String key);
    public Set<String> smembers(String key);
    public Long zremrangeByRank(String key ,long start, long end);
    public Long zremrangeByScore(String key,double start,double end);
    public String type(String key);
}
