package com.jut.redis;

import com.jut.redis.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/***
 * redis管理接口集群实现
 */
public class ClusterRedisManager implements RedisManager {
    private final static Logger logger = LoggerFactory.getLogger(ClusterRedisManager.class);

    @Autowired
    JedisCluster jedisCluster;

    /**
     * 从连接池中获取一个redis连接
     */
    private JedisCluster getJedis(){
        return  jedisCluster;
    }
    /**
     * 异常处理方法
     */
    private void exceptionProcess(Exception e) {
        logger.error(StringUtil.logException(e));
    }
    /**
     * 回收一个redis连接
     */
    private void returnResource(JedisCluster jedis) {

    }

    public  String get(String key){
        String value = null;
        JedisCluster jedis = null;
        try {
            jedis= getJedis();
            value = jedis.get(key);
        } catch(Exception e){
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return value;
    }

    public  String set(String key,String value){
        JedisCluster jedis = null;
        String ans = null;
        try {
            jedis = getJedis();
            ans = jedis.set(key,value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
        return ans;
    }

    public  String setex(String key,int seconds,String value){
        JedisCluster jedis = null;
        String ans = null;
        try {
            jedis = getJedis();
            ans = jedis.setex(key,seconds,value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
        return ans;
    }

    public  Long setrange(String key,int offset,String value){
        JedisCluster jedis = null;

        try {
            jedis = getJedis();
            return jedis.setrange(key, offset, value);
        } catch (Exception e) {
            exceptionProcess(e);
            return 0L;
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
    }

    public List<String> mget(String...keys){
        JedisCluster jedis = null;
        List<String> values = null;
        try {
            jedis = getJedis();
            values = jedis.mget(keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
        return values;
    }

    public  String mset(String...keysvalues){
        JedisCluster jedis = null;
        String ans = null;
        try{
            jedis = getJedis();
            ans = jedis.mset(keysvalues);
        }catch (Exception e) {
            exceptionProcess(e);
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
        return ans;
    }

    public  Long append(String key,String value){
        JedisCluster jedis = null;

        try {
            jedis = getJedis();
            return jedis.append(key, value);
        } catch (Exception e) {
            exceptionProcess(e);
            return 0L;
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
    }

    public  Boolean exists(String key){
        JedisCluster jedis = null;
        try {
            jedis = getJedis();
            return jedis.exists(key);
        } catch (Exception e) {
            exceptionProcess(e);
            return false;
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
    }

    public  Long setnx(String key,String value){
        JedisCluster jedis = null;
        try {
            jedis = getJedis();
            return jedis.setnx(key,value);
        } catch (Exception e) {
            exceptionProcess(e);
            return 0l;
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
    }

    public Long expire(String key,int seconds){

        JedisCluster jedis = null;
        Long res = 0l;
        try {
            jedis = getJedis();
            res = jedis.expire(key, seconds);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
        return res;
    }

    public Long del(String...keys){

        JedisCluster jedis = null;
        Long res = 0l;
        try {
            jedis = getJedis();
            res = jedis.del(keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
        return res;
    }

    public Long timetolive(String key){
        JedisCluster jedis = null;
        Long res = 0l;
        try {
            jedis = getJedis();
            res = jedis.ttl(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            //返还到连接池
            returnResource(jedis);
        }
        return res;
    }

    public Long msetnx(String...keysvalues){
        JedisCluster jedis = null;
        Long res = 0L;
        try {
            jedis = getJedis();
            res =jedis.msetnx(keysvalues);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String getset(String key,String value){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.getSet(key, value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String getrange(String key, int startOffset ,int endOffset){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long incr(String key){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.incr(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long incrBy(String key,Long integer){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.incrBy(key, integer);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long decr(String key) {
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.decr(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long decrBy(String key,Long integer){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.decrBy(key, integer);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long serlen(String key){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.strlen(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long hset(String key,String field,String value) {
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.hset(key, field, value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long hsetnx(String key,String field,String value){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.hsetnx(key, field, value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String hmset(String key, Map<String, String> hash){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.hmset(key, hash);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String hget(String key, String field){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.hget(key, field);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public List<String> hmget(String key,String...fields){
        JedisCluster jedis = null;
        List<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.hmget(key, fields);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long hincrby(String key ,String field ,Long value){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.hincrBy(key, field, value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Boolean hexists(String key , String field){
        JedisCluster jedis = null;
        Boolean res = false;
        try {
            jedis = getJedis();
            res = jedis.hexists(key, field);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long hlen(String key){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.hlen(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;

    }

    public Long hdel(String key ,String...fields){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.hdel(key, fields);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Set<String> hkeys(String key){
        JedisCluster jedis = null;
        Set<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.hkeys(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public List<String> hvals(String key){
        JedisCluster jedis = null;
        List<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.hvals(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Map<String, String> hgetall(String key){
        JedisCluster jedis = null;
        Map<String, String> res = null;
        try {
            jedis = getJedis();
            res = jedis.hgetAll(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long lpush(String key ,String...strs){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.lpush(key, strs);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long rpush(String key ,String...strs){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.rpush(key, strs);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long linsert(String key, ListPosition where,
                        String pivot, String value){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.linsert(key, where, pivot, value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String lset(String key ,Long index, String value){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.lset(key, index, value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long lrem(String key,long count,String value){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.lrem(key, count, value);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String ltrim(String key ,long start ,long end){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.ltrim(key, start, end);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String lpop(String key){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.lpop(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String rpop(String key){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.rpop(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String rpoplpush(String srckey, String dstkey){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.rpoplpush(srckey, dstkey);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String lindex(String key,long index){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.lindex(key, index);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long llen(String key){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.llen(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public List<String> lrange(String key,long start,long end){
        JedisCluster jedis = null;
        List<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.lrange(key, start, end);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long sadd(String key,String...members){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.sadd(key, members);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long srem(String key,String...members){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.srem(key, members);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String spop(String key){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.spop(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Set<String> sdiff(String...keys){
        JedisCluster jedis = null;
        Set<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.sdiff(keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long sdiffstore(String dstkey,String... keys){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.sdiffstore(dstkey, keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Set<String> sinter(String...keys){
        JedisCluster jedis = null;
        Set<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.sinter(keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long sinterstore(String dstkey,String...keys){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.sinterstore(dstkey, keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Set<String> sunion(String... keys){
        JedisCluster jedis = null;
        Set<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.sunion(keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long sunionstore(String dstkey,String...keys){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.sunionstore(dstkey, keys);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long smove(String srckey, String dstkey, String member){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.smove(srckey, dstkey, member);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long scard(String key){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.scard(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Boolean sismember(String key,String member){
        JedisCluster jedis = null;
        Boolean res = null;
        try {
            jedis = getJedis();
            res = jedis.sismember(key, member);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String srandmember(String key){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.srandmember(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Set<String> smembers(String key){
        JedisCluster jedis = null;
        Set<String> res = null;
        try {
            jedis = getJedis();
            res = jedis.smembers(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long zremrangeByRank(String key ,long start, long end){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Long zremrangeByScore(String key,double start,double end){
        JedisCluster jedis = null;
        Long res = null;
        try {
            jedis = getJedis();
            res = jedis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public Set<String> keys(String pattern){
        JedisCluster jedis = null;
        Set<String> res = null;
        try {
            Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
            for(String k : clusterNodes.keySet()) {
                JedisPool jp = clusterNodes.get(k);
                Jedis connection = jp.getResource();
                res.addAll(connection.keys(pattern));
            }
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }

    public String type(String key){
        JedisCluster jedis = null;
        String res = null;
        try {
            jedis = getJedis();
            res = jedis.type(key);
        } catch (Exception e) {
            exceptionProcess(e);
        } finally {
            returnResource(jedis);
        }
        return res;
    }
}
