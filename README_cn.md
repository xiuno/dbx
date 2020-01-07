# dbx:  一个支持 MySQL/SQLite/Cassandra/ScyllaDB + KV 缓存全表数据的高性能 Golang DB 库

什么是 dbx ? 简而言之就是：
> **dbx = DB + Cache**

它是一个支持对 MySQL/SQLite/Cassandra/ScyllaDB 全表数据进行透明缓存的 Golang DB 库，在内存足够大的情况下，不再需要 Memcached, Redis 等缓存服务。
而且读取缓存的速度相当之快，本机测试 qps 达到:  350万+/秒，可以有效的简化应用端业务逻辑代码。
它支持 MySQL/Sqlite3，支持结构体自由组合嵌套。
它的实现原理为自动扫描表结构，确定主键和自增列，并且通过主键按照行来缓存数据，按照行透明管理 cache，上层只需要按照普通的 ORM 风格 API 操作即可。

# 支持缓存，高性能读取 KV 缓存全表数据
经过本机简单的测试（小数据下），直接查询 Sqlite3 速度可以达到 3万+/秒，开启缓存后达到恐怖的 350万+/秒。
一般针对高频访问的小表开启缓存：
```golang
db.Bind("user", &User{}, true)
db.Bind("group", &Group{}, true)
db.EnableCache(true)
```

# 支持嵌套，尽量避免低效反射
golang 为静态语言，在实现比较复杂的功能的时候往往要用到反射，而反射使用不当的时候会严重拖慢速度。经过实践发现，应该尽量使用数字索引，不要使用字符串索引，比如 Field() 性能大约是 FieldByName() 的 50 倍！
绝大部分 db 库不支持嵌套，因为反射又慢又复杂，特别是嵌套层数过多的时候。还好通过努力，dbx 高效的实现了无限制层数的嵌套支持，并且性能还不错。
```golang
type Human struct {
	Age int64     `db:"age"`
}
type User struct {
	Human
	Uid        int64     `db:"uid"`
	Gid        int64     `db:"gid"`
	Name       string    `db:"name"`
	CreateDate time.Time `db:"createDate"`
}
```

# API 预览：
通过 golang 的反射特性，可以实现接近脚本语言级的便捷程度。如下：
```golang

// 打开数据库
db, err = dbx.Open("mysql", "root@tcp(localhost)/test?parseTime=true&charset=utf8")

// 插入一条
db.Table("user").Insert(user)

// 查询一条
db.Table("user").Where("uid=?", 1).One(&user)

// 通过主健查询一条
db.Table("user").WherePK(1).One(&user)

// 通过主健更新一条
db.Table("user").Update(&user)

// 通过主健删除一条
db.Table("user").WherePK(1).Delete()

// 获取多条
db.Table("user").Where("uid>?", 1).All(&userList)

// IN() 获取多条
db.Table("user").Where("uid IN(?)", []int{1, 2, 3}).All(&userList)

```

# 日志输出到指定的流
可以自由的重定向日志数据流。
```golang
// 将 db 产生的错误信息输出到标准输出（控制台）
db.Stderr = os.Stdout

// 将 db 产生的错误信息输出到指定的文件
db.Stderr = dbx.OpenFile("./db_error.log") 

// 默认：将 db 的输出（主要为 SQL 语句）重定向到"黑洞"（不输出执行的 SQL 语句等信息）
db.Stdout = ioutil.Discard

// 默认：将 db 产生的输出（主要为 SQL 语句）输出到标准输出（控制台）
db.Stdout = os.Stdout
```

# 兼容原生的方法
有时候我们需要调用原生的接口，来实现比较复杂的目的。
```golang
// 自定义复杂 SQL 获取单条结果（原生）
var uid int64
err = db.QueryRow("SELECT uid FROM user WHERE uid=?", 2).Scan(&uid)
if err != nil {
	panic(err)
}
fmt.Printf("uid: %v\n", uid)
db.Table("user").LoadCache() // 自定义需要手动刷新缓存
```

# Cassandra/Scylladb 用例
```
db, err = dbx.Open("cql", "root@tcp(192.168.0.129:9042)/btc")
dbx.Check(err)
defer db.Close()
具体用法参考: example/test_cql/main.go
```

# MySQL/SQLite 用例
```golang
package main

import (
	"github.com/xiuno/dbx"
	"fmt"
	"os"
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

	// db, err = dbx.Open("sqlite3", "./db1.db?cache=shared&mode=rwc&parseTime=true&charset=utf8") // sqlite3
	db, err = dbx.Open("mysql", "root@tcp(localhost)/test?parseTime=true&charset=utf8")            // mysql
	dbx.Check(err)
	defer db.Close()

	// db 输出信息设置
	db.Stdout = os.Stdout // 将 db 产生的信息(大部分为 sql 语句)输出到标准输出
	db.Stderr = dbx.OpenFile("./db_error.log") // 将 db 产生的错误信息输出到指定的文件
	// db.Stdout = ioutil.Discard // 默认：将 db 的输出信息重定向到"黑洞"（不输出执行的 SQL 语句等信息）

	// 参数设置
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	// db.SetConnMaxLifetime(time.Second * 5)

	// 创建表
	_, err = db.Exec(`DROP TABLE IF EXISTS user;`)
	_, err = db.Exec(`CREATE TABLE user(
		uid        INT(11) PRIMARY KEY AUTO_INCREMENT,
		gid        INT(11) NOT NULL DEFAULT '0',
		name       TEXT             DEFAULT '',
		createDate DATETIME         DEFAULT CURRENT_TIMESTAMP
		);
	`)
	dbx.Check(err)

	// 开启缓存，可选项，一般只针对小表开启缓存，超过 10w 行，不建议开启！
	db.Bind("user2", &User{}, true)
	db.EnableCache(true)

	// 插入一条
	u1 := &User{1, 1, "jack", time.Now()}
	_, err = db.Table("user").Insert(u1)
	dbx.Check(err)

	// 读取一条
	u2 := &User{}
	err = db.Table("user").WherePK(1).One(u2)
	dbx.Check(err)
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
	dbx.Check(err)

	// 删除一条
	_, err = db.Table("user").WherePK(1).Delete()
	dbx.Check(err)

	// Where 条件 + 更新
	_, err = db.Table("user").WhereM(dbx.M{{"uid", 1}, {"gid", 1}}).UpdateM(dbx.M{{"Name", "jet.li"}})
	dbx.Check(err)

	// 插入多条
	for i := int64(0); i < 5; i++ {
		u := &User{
			Uid: i,
			Gid: i,
			Name: fmt.Sprintf("name-%v", i),
			CreateDate: time.Now(),
		}
		_, err := db.Table("user").Insert(u)
		if err != nil {
			panic(err)
		}
	}

	// 获取多条
	userList := []*User{}
	err = db.Table("user").Where("uid>?", 1).All(&userList)
	dbx.Check(err)
	for _, u := range userList {
		fmt.Printf("%+v\n", u)
	}

	// 批量更新
	_, err = db.Table("user").Where("uid>?", 3).UpdateM(dbx.M{{"gid", 10}})
	dbx.Check(err)

	// 批量删除
	_, err = db.Table("user").Where("uid>?", 3).Delete()
	if err != nil {
		panic(err)
	}

	// 总数
	n, err := db.Table("user").Where("uid>?", -1).Count()
	dbx.Check(err)
	fmt.Printf("count: %v\n", n)

	// 求和
	n, err = db.Table("user").Where("uid>?", -1).Sum("uid")
	dbx.Check(err)
	fmt.Printf("sum(uid): %v\n", n)

	// 求最大值
	n, err = db.Table("user").Where("uid>?", -1).Max("uid")
	dbx.Check(err)
	fmt.Printf("max(uid): %v\n", n)

	// 求最小值
	n, err = db.Table("user").Where("uid>?", -1).Min("uid")
	dbx.Check(err)
	fmt.Printf("min(uid): %v\n", n)

	// 自定义复杂 SQL 获取单条结果（原生）
	var uid int64
	err = db.QueryRow("SELECT uid FROM user WHERE uid=?", 2).Scan(&uid)
	dbx.Check(err)
	fmt.Printf("uid: %v\n", uid)
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	// 自定义复杂 SQL 获取多条（原生）
	var name string
	rows, err := db.Query("SELECT `uid`, `name` FROM `user` WHERE 1 ORDER BY uid DESC")
	dbx.Check(err)
	rows.Close()
	for rows.Next() {
		rows.Scan(&uid, &name)
		fmt.Printf("uid: %v, name: %v\n", uid, name)
	}
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	return
}

```
[Document for English](README.md)
