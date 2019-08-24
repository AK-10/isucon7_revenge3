package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"

	"github.com/labstack/echo"
)

const (
	ICONS_PATH = "/home/isucon/isubata/webapp/public/icons/"
)

func jsonifyMessage(m Message) (map[string]interface{}, error) {
	u := User{}
	err := db.Get(&u, "SELECT name, display_name, avatar_icon FROM user WHERE id = ?",
		m.UserID)
	if err != nil {
		return nil, err
	}

	r := make(map[string]interface{})
	r["id"] = m.ID
	r["user"] = u
	r["date"] = m.CreatedAt.Format("2006/01/02 15:04:05")
	r["content"] = m.Content
	return r, nil
}

func (r *Renderer) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return r.templates.ExecuteTemplate(w, name, data)
}

const LettersAndDigits = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(n int) string {
	b := make([]byte, n)
	z := len(LettersAndDigits)

	for i := 0; i < n; i++ {
		b[i] = LettersAndDigits[rand.Intn(z)]
	}
	return string(b)
}

func tAdd(a, b int64) int64 {
	return a + b
}

func tRange(a, b int64) []int64 {
	r := make([]int64, b-a+1)
	for i := int64(0); i <= (b - a); i++ {
		r[i] = a + i
	}
	return r
}

func writeFile(filename string, data []byte) error {
	filepath := ICONS_PATH + filename
	err := ioutil.WriteFile(filepath, data, 0644)
	if err != nil {
		return err
	}
	return nil
}

func initializeImagesInDB() error {
	rows, err := db.Query("SELECT name, data FROM image")
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return err
		}
		err = writeFile(name, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func getLastNArray(arr []string, n int) []string {
	var length int
	length = len(arr)
	ans := make([]string, 0, n)
	if length > n {
		index := length - 1
		for i := 0; i < n; i++ {
			ans = append(ans, arr[index])
			index--
		}
	} else {
		for i := length - 1; i >= 0; i-- {
			ans = append(ans, arr[i])
		}
	}
	return ans
}

func selectStringArray(arr []string, lastID int64, f func(string, int) bool) []string {
	ans := make([]string, 0)
	i := int(lastID)
	for _, x := range arr {
		if f(x, i) == true {
			ans = append(ans, x)
		}
	}
	return ans
}
