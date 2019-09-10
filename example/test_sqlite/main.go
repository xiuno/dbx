package main

import (
	"fmt"
	"github.com/xiuno/dbx"
	"os"
	"time"
)

type Human struct {
	Uid        int64     `db:"uid"`
	Gid        int64     `db:"gid"`
}

type User struct {
	*Human
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
	db.Stdout = os.Stdout // 默认：将 db 产生的错误信息输出到标准输出
	db.Stderr = dbx.OpenFile("./db1_error.log") // 将 db 产生的错误信息输出到指定的文件
	// db.Stdout = ioutil.Discard // 默认：将 db 的输出信息重定向到"黑洞"（不输出执行的 SQL 语句等信息）

	// 参数设置
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	//db.SetConnMaxLifetime(time.Second * 5) // 不使用的链接关闭掉，快速回收。

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
	u1 := &User{&Human{1, 1}, "jack", time.Now()}
	_, err = db.Table("user").Insert(u1)
	if err != nil {
		panic(err)
	}

	// 读取一条
	u2 := &User{}
	u2.Human = &Human{}
	err = db.Table("user").WherePK(1).One(u2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", u2)

	// 读取一条，判断是否存在
	err = db.Table("user").WherePK(1).One(u2)
	dbx.Check(err)
	if dbx.NoRows(err) {
		panic("not found.")
	}
	fmt.Printf("%+v\n", u2)

	// 更新一条
	u2.Name = "jack.ma"
	_, err = db.Table("user").Update(u2)
	if err != nil {
		panic(err)
	}

	// 删除一条
	_, err = db.Table("user").WherePK(1).Delete()
	if err != nil {
		panic(err)
	}

	// 插入多条
	for i := int64(0); i < 5; i++ {
		u := &User{
			Name: fmt.Sprintf("name-%v", i),
			CreateDate: time.Now(),
		}
		u.Human = &Human{}
		u.Human.Uid = i
		u.Human.Gid = i
		_, err := db.Table("user").Insert(u)
		if err != nil {
			panic(err)
		}
	}

	// 获取多条
	userList := []*User{}
	err = db.Table("user").Where("uid>?", 1).All(&userList)
	if err != nil {
		panic(err)
	}
	for _, u := range userList {
		fmt.Printf("%+v, %+v\n", u.Human, u)
	}

	// 批量更新
	_, err = db.Table("user").Where("uid>?", 3).UpdateM(dbx.M{{"gid", 10}})
	if err != nil {
		panic(err)
	}

	// 批量删除
	_, err = db.Table("user").Where("uid>?", 3).Delete()
	if err != nil {
		panic(err)
	}

	// 总数
	n, err := db.Table("user").Where("uid>?", -1).Count()
	if err != nil {
		panic(err)
	}
	fmt.Printf("count: %v\n", n)

	// 求和
	n, err = db.Table("user").Where("uid>?", -1).Sum("uid")
	if err != nil {
		panic(err)
	}
	fmt.Printf("sum(uid): %v\n", n)

	// 求最大值
	n, err = db.Table("user").Where("uid>?", -1).Max("uid")
	if err != nil {
		panic(err)
	}
	fmt.Printf("max(uid): %v\n", n)

	// 求最小值
	n, err = db.Table("user").Where("uid>?", -1).Min("uid")
	if err != nil {
		panic(err)
	}
	fmt.Printf("min(uid): %v\n", n)

	// 自定义复杂 SQL 获取单条结果（原生）
	var uid int64
	err = db.QueryRow("SELECT uid FROM user WHERE uid=?", 2).Scan(&uid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("uid: %v\n", uid)
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	// 自定义复杂 SQL 获取多条（原生）
	var name string
	rows, err := db.Query("SELECT `uid`, `name` FROM `user` WHERE 1 ORDER BY uid DESC")
	if err != nil {
		panic(err)
	}
	rows.Close()
	for rows.Next() {
		rows.Scan(&uid, &name)
		fmt.Printf("uid: %v, name: %v\n", uid, name)
	}
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	return
}
