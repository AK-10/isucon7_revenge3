package main

import (
	"encoding/json"
	"fmt"
	"log"
	// "strconv"
	"errors"
	"strings"

	"github.com/gomodule/redigo/redis"
)

const (
	redisHost = "127.0.0.1"
	redisPort = "6379"
)

var (
	// 取得しようとしてるキーに対して、オペレーションが違うときのエラー
	WrongTypeError = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type Redisful struct {
	Conn redis.Conn
}

// func main() {
// 	r, _ := NewRedisful()
// 	r.Transaction(func() {
// 		r.SetDataToCache("STRING", "string")
// 		r.SetDataToCache("STR", "string")
// 	})
// }

func NewRedisful() (*Redisful, error) {
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", redisHost, redisPort))
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &Redisful{
		Conn: conn,
	}, nil
}

func (r *Redisful) Close() error {
	return r.Conn.Close()
}

func (r *Redisful) FLUSH_ALL() error {
	r.Conn.Do("FLUSHALL")
	return nil
}

func (r *Redisful) Transaction(tx func()) error {
	_, err := r.Conn.Do("MULTI")
	if err != nil {
		return err
	}

	tx()

	_, err = r.Conn.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

// =====================
//		string型
// =====================

func (r *Redisful) GetDataFromCache(key string) ([]byte, error) {
	data, err := redis.Bytes(r.Conn.Do("GET", key))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return nil, err
	}
	return data, nil
}

// SETはkeyが存在する場合上書きしてしまう
func (r *Redisful) SetDataToCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("SET", key, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

// SETNXはkeyが存在しない場合のみ挿入
func (r *Redisful) SetNXDataToCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("SETNX", key, data)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redisful) IncrementDataInCache(key string) error {
	_, err := r.Conn.Do("INCR", key)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redisful) DecrementDataInCache(key string) error {
	_, err := r.Conn.Do("DECR", key)
	if err != nil {
		return err
	}
	return nil
}

// ===========================
// 			List 型
// ===========================

func (r *Redisful) GetListFromCache(key string) ([]byte, error) {
	strs, err := redis.Strings(r.Conn.Do("LRANGE", key, 0, -1))
	if err != nil {
		return nil, err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	return []byte(str), err
}

func (r *Redisful) GetListLangeFromCache(key string, start, end int) ([]byte, error) {
	strs, err := redis.Strings(r.Conn.Do("LRANGE", key, start, end))
	if err != nil {
		return nil, err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	return []byte(str), err
}

// RPUSHは最後に追加
func (r *Redisful) RPushListToCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("RPUSH", key, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

func (r *Redisful) LPushListToCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("LPUSH", key, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

// マッチするものを1つ削除
func (r *Redisful) RemoveListFromCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("LREM", key, 1, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

func (r *Redisful) GetListLengthInCache(key string) (int64, error) {
	count, err := r.Conn.Do("LLEN", key)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return 0, err
	}
	return count.(int64), nil
}

// =============================
// 			ハッシュ型
// =============================
func (r *Redisful) SetHashToCache(key, field string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("HSET", key, field, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

func (r *Redisful) GetHashFromCache(key, field string) ([]byte, error) {
	data, err := redis.Bytes(r.Conn.Do("HGET", key, field))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return nil, err
	}
	return data, nil
}

func (r *Redisful) RemoveHashFromCache(key, field string) error {
	_, err := r.Conn.Do("HDEL", key, field)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

// 入力された順
func (r *Redisful) GetAllHashFromCache(key string) ([]byte, error) {
	strs, err := redis.Strings(r.Conn.Do("HVALS", key))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return nil, err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	return []byte(str), err
}

func (r *Redisful) GetMultiFromCache(key string, fields []string) ([]byte, error) {
	// conn.Doの引数に合うように変換
	querys := make([]interface{}, 0, len(fields)+1)
	querys = append(querys, key)
	for i := range fields {
		querys = append(querys, fields[i])
	}

	fmt.Println(querys...)
	strs, err := redis.Strings((r.Conn.Do("HMGET", querys...)))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return nil, err
	}

	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	return []byte(str), nil
}

// redis.ErrNilを返さない
// keyがない場合は、0を返す
func (r *Redisful) GetHashLengthInCache(key string) (int64, error) {
	count, err := r.Conn.Do("HLEN", key)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return 0, err
	}
	return count.(int64), nil
}

func (r *Redisful) GetHashKeysInCache(key string) ([]string, error) {
	data, err := redis.Strings(r.Conn.Do("HKEYS", key))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return []string{}, err
	}
	return data, nil
}

// ===================
//		 Set 型
// ===================
func (r *Redisful) GetSetFromCache(key string) ([]byte, error) {
	strs, err := redis.Strings(r.Conn.Do("SMEMBERS", key))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return nil, err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	return []byte(str), err
}

func (r *Redisful) PushSetToCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = r.Conn.Do("SADD", key, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

// マッチするものを1つ削除
func (r *Redisful) RemoveSetFromCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	_, err = r.Conn.Do("SREM", key, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

func (r *Redisful) GetSetLengthFromCache(key string) (int64, error) {
	count, err := r.Conn.Do("SCARD", key)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return 0, err
	}
	return count.(int64), nil
}

// =========================
//		 Sorted Set 型
// =========================
func (r *Redisful) GetSortedSetFromCache(key string, desc bool) ([]byte, error) {
	var strs []string
	var err error
	if desc {
		strs, err = redis.Strings(r.Conn.Do("ZRANGE", key, 0, -1))
	} else {
		strs, err = redis.Strings(r.Conn.Do("ZREVRANGE", key, 0, -1))
	}
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return nil, err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	return []byte(str), err
}

func (r *Redisful) PushSortedSetToCache(key string, score int, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("ZADD", key, score, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

// マッチするものを1つ削除
func (r *Redisful) RemoveSortedSetFromCache(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.Conn.Do("ZREM", key, data)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	return nil
}

func (r *Redisful) GetSortedSetLengthFromCache(key string) (int64, error) {
	count, err := r.Conn.Do("ZCARD", key)
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return 0, err
	}
	return count.(int64), nil
}
