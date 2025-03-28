package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// キャッシュアイテム構造体
type CacheItem struct {
	Key   string
	Score float64
	Value interface{}
}

// SortedSetCache はソート済みセットを使用したキャッシュの実装
type SortedSetCache struct {
	client     *redis.Client
	setKey     string
	expiration time.Duration
	ctx        context.Context
}

// NewSortedSetCache は新しいソート済みセットキャッシュを作成します
func NewSortedSetCache(addr, password string, db int, setKey string, expiration time.Duration) (*SortedSetCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()

	// 接続テスト
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("Redis接続エラー: %v", err)
	}

	return &SortedSetCache{
		client:     client,
		setKey:     setKey,
		expiration: expiration,
		ctx:        ctx,
	}, nil
}

// Set はアイテムをキャッシュに追加します
func (c *SortedSetCache) Set(key string, value interface{}, score float64) error {
	strValue, ok := value.(string)
	if !ok {
		return fmt.Errorf("キャッシュ値は文字列である必要があります")
	}

	// ZAddを使用してソート済みセットにアイテムを追加
	member := redis.Z{
		Score:  score,
		Member: strValue,
	}

	_, err := c.client.ZAdd(c.ctx, c.setKey, member).Result()
	if err != nil {
		return fmt.Errorf("キャッシュ追加エラー: %v", err)
	}

	// キーと値のペアをセット
	err = c.client.Set(c.ctx, key, strValue, c.expiration).Err()
	if err != nil {
		return fmt.Errorf("キャッシュセットエラー: %v", err)
	}

	return nil
}

// GetByScoreRange はスコア範囲内のアイテムを取得します
func (c *SortedSetCache) GetByScoreRange(min, max float64, offset, count int64) ([]CacheItem, error) {
	opt := &redis.ZRangeBy{
		Min:    fmt.Sprintf("%f", min),
		Max:    fmt.Sprintf("%f", max),
		Offset: offset,
		Count:  count,
	}

	result, err := c.client.ZRangeByScoreWithScores(c.ctx, c.setKey, opt).Result()
	if err != nil {
		return nil, fmt.Errorf("スコア範囲内アイテム取得エラー: %v", err)
	}

	items := make([]CacheItem, 0, len(result))
	for _, z := range result {
		key, ok := z.Member.(string)
		if !ok {
			continue
		}

		val, err := c.Get(key)
		if err != nil {
			continue
		}

		items = append(items, CacheItem{
			Key:   key,
			Score: z.Score,
			Value: val,
		})

	}
	return items, nil
}

func (c *SortedSetCache) Get(key string) (interface{}, error) {
	val, err := c.client.Get(c.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("キャッシュにアイテムが存在しません: %s", key)
		}
		return nil, fmt.Errorf("キャッシュ取得エラー: %v", err)
	}
	return val, nil
}

// Close はRedis接続を閉じます
func (c *SortedSetCache) Close() error {
	return c.client.Close()
}

func main() {
	// キャッシュの作成
	cache, err := NewSortedSetCache("localhost:6379", "", 0, "my-sorted-cache", 1*time.Hour)
	if err != nil {
		log.Fatalf("キャッシュの初期化エラー: %v", err)
	}
	defer cache.Close()

	// キャッシュにアイテムを追加
	err = cache.Set("item1", "Value for item 1", 100)
	if err != nil {
		log.Printf("アイテム追加エラー: %v", err)
	}

	err = cache.Set("item2", "Value for item 2", 200)
	if err != nil {
		log.Printf("アイテム追加エラー: %v", err)
	}

	err = cache.Set("item3", "Value for item 3", 150)
	if err != nil {
		log.Printf("アイテム追加エラー: %v", err)
	}

	// スコア範囲内のアイテムを取得
	rangeItems, err := cache.GetByScoreRange(120, 250, 0, 10)
	if err != nil {
		log.Printf("スコア範囲内アイテム取得エラー: %v", err)
	} else {
		fmt.Println("\nスコア120〜250の範囲内のアイテム:")
		for _, item := range rangeItems {
			fmt.Printf("  キー: %s, スコア: %.1f, 値: %v\n", item.Key, item.Score, item.Value)
		}
	}
}
