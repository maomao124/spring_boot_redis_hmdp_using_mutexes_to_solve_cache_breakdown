package mao.spring_boot_redis_hmdp.service.impl;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import mao.spring_boot_redis_hmdp.dto.Result;
import mao.spring_boot_redis_hmdp.entity.Shop;
import mao.spring_boot_redis_hmdp.mapper.ShopMapper;
import mao.spring_boot_redis_hmdp.service.IShopService;
import mao.spring_boot_redis_hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService
{

    @Resource
    StringRedisTemplate stringRedisTemplate;


    @Override
    public Result queryShopById(Long id)
    {
        //查询
        Shop shop = this.queryWithMutex(id);
        //判断
        if (shop == null)
        {
            //不存在
            return Result.fail("店铺信息不存在");
        }
        //返回
        return Result.ok(shop);
    }

    /**
     * 互斥锁解决缓存击穿问题
     *
     * @param id 商铺id
     * @return Shop
     */
    private Shop queryWithMutex(Long id)
    {
        //获取redisKey
        String redisKey = RedisConstants.CACHE_SHOP_KEY + id;
        //从redis中查询商户信息，根据id
        String shopJson = stringRedisTemplate.opsForValue().get(redisKey);
        //判断取出的数据是否为空
        if (StrUtil.isNotBlank(shopJson))
        {
            //不是空，redis里有，返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //是空串，不是null，返回
        if (shopJson != null)
        {
            return null;
        }
        //锁的key
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;

        Shop shop = null;
        try
        {
            //获取互斥锁
            boolean lock = tryLock(lockKey);
            //判断锁是否获取成功
            if (!lock)
            {
                //没有获取到锁
                //200毫秒后再次获取
                Thread.sleep(200);
                //递归调用
                return queryWithMutex(id);
            }
            //得到了锁
            //null，查数据库
            shop = this.getById(id);
            //判断数据库里的信息是否为空
            if (shop == null)
            {
                //空，将空值写入redis，返回错误
                stringRedisTemplate.opsForValue().set(redisKey, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //存在，回写到redis里，设置随机的过期时间
            stringRedisTemplate.opsForValue().set(redisKey, JSONUtil.toJsonStr(shop),
                    RedisConstants.CACHE_SHOP_TTL * 60 + getIntRandom(0, 300), TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            //释放锁
            //System.out.println("释放锁");
            this.unlock(lockKey);
        }
        //返回数据
        return shop;
    }

    @Override
    public Result updateShop(Shop shop)
    {
        //获得id
        Long id = shop.getId();
        //判断是否为空
        if (id == null)
        {
            return Result.fail("商户id不能为空");
        }
        //不为空
        //先更新数据库
        boolean b = this.updateById(shop);
        //更新失败，返回
        if (!b)
        {
            return Result.fail("更新失败");
        }
        //更新没有失败
        //删除redis里的数据，下一次查询时自动添加进redis
        //redisKey
        String redisKey = RedisConstants.CACHE_SHOP_KEY + id;
        stringRedisTemplate.delete(redisKey);
        //返回响应
        return Result.ok();
    }

    /**
     * 获取一个随机数，区间包含min和max
     *
     * @param min 最小值
     * @param max 最大值
     * @return int 型的随机数
     */
    @SuppressWarnings("all")
    private int getIntRandom(int min, int max)
    {
        if (min > max)
        {
            min = max;
        }
        return min + (int) (Math.random() * (max - min + 1));
    }

    /**
     * 获取锁
     *
     * @param key redisKey
     * @return 获取锁成功，返回true，否则返回false
     */
    private boolean tryLock(String key)
    {
        Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",
                RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(result);
    }

    /**
     * 释放锁
     *
     * @param key redisKey
     */
    private void unlock(String key)
    {
        stringRedisTemplate.delete(key);
    }

}
