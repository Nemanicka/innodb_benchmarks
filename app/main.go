package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	//    "time"

	_ "github.com/go-sql-driver/mysql"
)

var rowsCount = 5000000

func deleteData(ctx context.Context, conn *sql.Conn, tableName string) {

	_, deleteError := conn.QueryContext(ctx, `DELETE FROM `+tableName)
	if deleteError != nil {
		log.Fatal(deleteError)
	}
}

func prepareStmt(ctx context.Context, conn *sql.Conn, tableName string) *sql.Stmt {
	stmtIns, err := conn.PrepareContext(ctx, "INSERT INTO "+tableName+" VALUES( ?, ? )")
	if err != nil {
		log.Fatal(err.Error(), " | prepare error")
	}
	return stmtIns
}

func insertData(ctx context.Context, conn *sql.Conn, tableName string) {
	date := time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)

	var tx *sql.Tx
	txOptions := sql.TxOptions{}

	for i := 0; i < rowsCount; i++ {
		if i%10000 == 0 {
			fmt.Println(i)
			if tx != nil {
				commitErr := tx.Commit()
				if commitErr != nil {
					log.Fatal(commitErr)
				}
			}

			var txErr error
			tx, txErr = conn.BeginTx(ctx, &txOptions)
			if txErr != nil {
				fmt.Println(txErr)
			}
		}
		date = date.Add(1 * time.Minute)

		query := fmt.Sprintf("INSERT INTO %s VALUES( ?, ? )", tableName)

		_, err := tx.Exec(query, i, date)
		if err != nil {
			if strings.Contains(err.Error(), "Duplicate entry") {
				fmt.Println("Duplicate", i)
				continue
			}
			log.Fatal(err.Error(), " | insert error")
		}
	}
}

// func checkRowsCount(ctx context.Context, conn *sql.Conn, tableName string) {
// 	var actualRowsCount int
// 	err := conn.QueryRowContext(ctx, "Select Count(id) FROM "+tableName).Scan(&actualRowsCount)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	if actualRowsCount != int(rowsCount) {
// 		log.Fatalln("Expected rows count:", actualRowsCount, ", actual rows count:", rowsCount)
// 	}
// }

func createData(ctx context.Context, conn *sql.Conn) {
	_, tableCreateError := conn.QueryContext(ctx, "CREATE TABLE IF NOT EXISTS UsersNoIndex ( id INT UNIQUE, birthDate DATE)")
	if tableCreateError != nil {
		log.Fatal(tableCreateError)
	}

	_, tableCreateError = conn.QueryContext(ctx, `CREATE TABLE IF NOT EXISTS UsersHashIndex ( id INT UNIQUE, 
        birthDate DATE,  INDEX birthDate_hash (birthDate) USING HASH)`)
	if tableCreateError != nil {
		log.Fatal(tableCreateError)
	}

	_, tableCreateError = conn.QueryContext(ctx, `CREATE TABLE IF NOT EXISTS UsersBtreeIndex ( id INT UNIQUE, 
        birthDate DATE,  INDEX birthDate_btree (birthDate))`)
	if tableCreateError != nil {
		log.Fatal(tableCreateError)
	}

	noIndexStartDelete := time.Now()
	deleteData(ctx, conn, "UsersNoIndex")
	noIndexEndDelete := time.Now()
	HashIndexStartDelete := time.Now()
	deleteData(ctx, conn, "UsersHashIndex")
	HashIndexEndDelete := time.Now()
	BtreeIndexStartDelete := time.Now()
	deleteData(ctx, conn, "UsersBtreeIndex")
	BtreeIndexEndDelete := time.Now()

	// stmtInsNoIndex := prepareStmt(ctx, conn, "UsersNoIndex")
	// defer stmtInsNoIndex.Close()

	// stmtInsHashIndex := prepareStmt(ctx, conn, "UsersHashIndex")
	// defer stmtInsHashIndex.Close()

	// stmtInsBtreeIndex := prepareStmt(ctx, conn, "UsersBtreeIndex")
	// defer stmtInsBtreeIndex.Close()

	// Verify rows count

	noIndexStartInsert := time.Now()
	insertData(ctx, conn, "UsersNoIndex")
	noIndexEndInsert := time.Now()

	// checkRowsCount(ctx, conn, "UsersNoIndex")

	HashIndexStartInsert := time.Now()
	insertData(ctx, conn, "UsersHashIndex")
	HashIndexEndInsert := time.Now()

	// checkRowsCount(ctx, conn, "UsersHashIndex")

	BtreeIndexStartInsert := time.Now()
	insertData(ctx, conn, "UsersBtreeIndex")
	BtreeIndexEndInsert := time.Now()

	// checkRowsCount(ctx, conn, "UsersBtreeIndex")

	fmt.Println("Insert Execution time:")
	fmt.Println("No Index   : ", noIndexEndInsert.Sub(noIndexStartInsert).Seconds())
	fmt.Println("Hash Index : ", HashIndexEndInsert.Sub(HashIndexStartInsert).Seconds())
	fmt.Println("Btree Index: ", BtreeIndexEndInsert.Sub(BtreeIndexStartInsert).Seconds())

	fmt.Println("Delete Execution time:")
	fmt.Println("No Index   : ", noIndexEndDelete.Sub(noIndexStartDelete).Seconds())
	fmt.Println("Hash Index : ", HashIndexEndDelete.Sub(HashIndexStartDelete).Seconds())
	fmt.Println("Btree Index: ", BtreeIndexEndDelete.Sub(BtreeIndexStartDelete).Seconds())
}

func selectData(ctx context.Context, conn *sql.Conn) {
	selectTableData(ctx, conn, "UsersNoIndex")
}

func selectTableData(ctx context.Context, conn *sql.Conn, tableName string) {
	var actualRowsCount int
	err := conn.QueryRowContext(ctx, "Select count(birthDate) FROM "+tableName).Scan(&actualRowsCount)
	if err != nil {
		log.Fatal(err)
	}

	if actualRowsCount != int(rowsCount) {
		log.Fatalln("Expected rows count:", actualRowsCount, ", actual rows count:", rowsCount)
	}
}

func main() {
	if len(os.Args[1:]) == 0 {
		fmt.Println("Please enter arguments: create|select")
		return
	}

	db, err := sql.Open("mysql", "root:pass@/mysql")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	conn, _ := db.Conn(ctx)

	if os.Args[1] == "create" {
		createData(ctx, conn)
	}

	if os.Args[1] == "select" {
		selectData(ctx, conn)
	}
}
