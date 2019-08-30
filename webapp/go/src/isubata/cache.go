package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	// "strconv"
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
// 	r.Close()
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

func (r *Redisful) GetDataFromCache(key string, v interface{}) error {
	data, err := redis.Bytes(r.Conn.Do("GET", key))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	err = json.Unmarshal(data, &v)
	return err
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
func (r *Redisful) SetNXDataToCache(key string, v interface{}) (bool, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	ok, err := redis.Bool(r.Conn.Do("SETNX", key, data))
	if err != nil {
		return false, err
	}
	return ok, nil
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

func (r *Redisful) GetListFromCache(key string, v interface{}) error {
	strs, err := redis.Strings(r.Conn.Do("LRANGE", key, 0, -1))
	if err != nil {
		return err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	err = json.Unmarshal([]byte(str), &v)
	return err
}

func (r *Redisful) GetListRangeFromCache(key string, start, end int, v interface{}) error {
	strs, err := redis.Strings(r.Conn.Do("LRANGE", key, start, end))
	if err != nil {
		return err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	err = json.Unmarshal([]byte(str), &v)
	return err
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

func (r *Redisful) SetNXHashToCache(key, field string, v interface{}) (bool, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	ok, err := redis.Bool(r.Conn.Do("HSET", key, field, data))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return false, err
	}
	return ok, nil
}

func (r *Redisful) GetHashFromCache(key, field string, v interface{}) error {
	data, err := redis.Bytes(r.Conn.Do("HGET", key, field))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	err = json.Unmarshal(data, &v)
	return err
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
func (r *Redisful) GetAllHashFromCache(key string, v interface{}) error {
	strs, err := redis.Strings(r.Conn.Do("HVALS", key))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	err = json.Unmarshal([]byte(str), &v)
	return err
}

func (r *Redisful) GetMultiFromCache(key string, fields []string, v interface{}) error {
	// conn.Doの引数に合うように変換
	if len(fields) == 0 {
		return errors.New("ERR wrong number of arguments for 'hmget' command")
	}
	querys := make([]interface{}, 0, len(fields)+1)
	querys = append(querys, key)
	for i := range fields {
		querys = append(querys, fields[i])
	}

	strs, err := redis.Strings((r.Conn.Do("HMGET", querys...)))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}

	strs = ignoreEmptyString(strs)
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	err = json.Unmarshal([]byte(str), &v)
	return err
}

func ignoreEmptyString(arr []string) []string {
	ans := make([]string, 0, len(arr))
	for _, v := range arr {
		if v != "" {
			ans = append(ans, v)
		}
	}
	return ans
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
func (r *Redisful) GetSetFromCache(key string, v interface{}) error {
	strs, err := redis.Strings(r.Conn.Do("SMEMBERS", key))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"
	json.Unmarshal([]byte(str), &v)

	return err
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
func (r *Redisful) GetSortedSetFromCache(key string, desc bool, v interface{}) error {
	var strs []string
	var err error
	if desc {
		strs, err = redis.Strings(r.Conn.Do("ZREVRANGE", key, 0, -1))
	} else {
		strs, err = redis.Strings(r.Conn.Do("ZRANGE", key, 0, -1))
	}
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	err = json.Unmarshal([]byte(str), &v)

	return err
}

func (r *Redisful) GetSortedSetRankRangeFromCache(key string, min, max int, desc bool, v interface{}) error {
	var strs []string
	var err error
	if desc {
		strs, err = redis.Strings(r.Conn.Do("ZREVRANGEBYSCORE", key, max, min))
	} else {
		strs, err = redis.Strings(r.Conn.Do("ZRANGEBYSCORE", key, min, max))
	}
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return err
	}
	str := strings.Join(strs[:], ",")
	str = "[" + str + "]"

	err = json.Unmarshal([]byte(str), &v)
	return err
}

func (r *Redisful) PushSortedSetToCache(key string, score int, v interface{}) (bool, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	ok, err := redis.Bool(r.Conn.Do("ZADD", key, score, data))
	if err != nil {
		if err.Error() == WrongTypeError.Error() {
			log.Fatal(err)
		}
		return false, err
	}
	return ok, nil
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
