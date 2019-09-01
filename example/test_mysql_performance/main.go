package main

import (
	"fmt"
	"github.com/mydeeplike/dbx"
	"time"
)

type User struct {
	Uid        int64     `db:"uid"`
	Gid        int64     `db:"gid"`
	Name       string    `db:"name"`
	CreateDate time.Time `db:"createDate"`
}

func main() {

	var err error
	var db *dbx.DB

	// db, err = dbx.Open("mysql", "root:root@tcp(localhost)/test?parseTime=true&charset=utf8")
	db, err = dbx.Open("mysql", "root@tcp(localhost)/test?parseTime=true&charset=utf8")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// db 输出信息设置
	//db.Stdout = os.Stdout // 默认：将 db 产生的错误信息输出到标准输出
	db.Stderr = dbx.OpenFile("./db_error.log") // 将 db 产生的错误信息输出到指定的文件
	// db.Stdout = ioutil.Discard // 默认：将 db 的输出信息重定向到"黑洞"（不输出执行的 SQL 语句等信息）

	// 参数设置
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	//db.SetConnMaxLifetime(time.Second * 5)

	// 创建表
	_, err = db.Exec(`DROP TABLE IF EXISTS user;`)
	_, err = db.Exec(`CREATE TABLE user(
		uid        INT(11) PRIMARY KEY AUTO_INCREMENT,
		gid        INT(11) NOT NULL DEFAULT '0',
		name       TEXT             DEFAULT '',
		createDate DATETIME         DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		panic(err)
	}

	// 开启缓存，可选项，一般只针对小表开启缓存，超过 10w 行，不建议开启！
	db.Bind("user", &User{}, true)
	db.EnableCache(true)

	// 插入一条
	u1 := &User{1, 1, "jack", time.Now()}
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
