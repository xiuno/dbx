package main

import (
	"fmt"
	"github.com/mydeeplike/dbx"
	//"os"
	"time"
)

type Human struct {
	Uid        int64     `db:"uid"`
	Gid        int64     `db:"gid"`
}

type User struct {
	Human
	Name       string    `db:"name"`
	CreateDate time.Time `db:"createDate"`
}

func main() {

	var err error
	var db *dbx.DB

	db, err = dbx.Open("sqlite3", "./db1.db?cache=shared&mode=rwc&parseTime=true&charset=utf8")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// db 输出信息设置
	//db.Stdout = os.Stdout // 默认：将 db 产生的错误信息输出到标准输出
	db.Stderr = dbx.OpenFile("./db1_error.log") // 将 db 产生的错误信息输出到指定的文件
	// db.Stdout = ioutil.Discard // 默认：将 db 的输出信息重定向到"黑洞"（不输出执行的 SQL 语句等信息）

	// 参数设置
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	//db.SetConnMaxLifetime(time.Second * 5)

	//db.Exec("PRAGMA journal_mode=WAL;")
	//db.Exec("PRAGMA wal_autocheckpoint=100;")

	// 创建表
	_, err = db.Exec(`
		DROP TABLE IF EXISTS user;
		CREATE TABLE user
		(
		  uid        INTEGER PRIMARY KEY AUTOINCREMENT,
		  gid        INTEGER NOT NULL DEFAULT '0',
		  name       TEXT             DEFAULT '',
		  createDate DATETIME         DEFAULT CURRENT_TIMESTAMP
		);`)
	if err != nil {
		panic(err)
	}

	// 插入一条
	u1 := &User{Human{1, 1}, "jack", time.Now()}
	_, err = db.Table("user").Insert(u1)
	if err != nil {
		panic(err)
	}


	db.Bind("user", &User{}, true)
	db.EnableCache(false)

	// 性能测试
	u2 := &User{}
	t1 := time.Now()
	for i := 0; i < 40000; i++ {
		db.Table("user").WherePK(i).One(u2)
	}
	fmt.Printf("%v\n", time.Now().Sub(t1))
	return
}
