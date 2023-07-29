# innodb_benchmarks

## Selection benchmarks for 5 million entries.

| Index Type | Execution Time |
|------------|----------------|
| No Index   | 0.7841643      |
| Hash Index | 0.0186594      |
| Btree Index| 0.0179023      |

## Insertion benchmarks for 5 million entries

| Index Type | Execution Time trx_commit 2 | Execution Time trx_commit 1 | Execution Time trx_commit 0 |
|------------|-----------------------------|-----------------------------|-----------------------------|
| No Index   | 2180.6530592                | 16933.2999406               | 2128.2172231                |
| Hash Index | 2193.4942367                | 17117.7314383               | 2128.8227601                |
| Btree Index| 2283.5473962                | 17072.4532764               | 2090.8328692                |


## Create statements

*No index
``` CREATE TABLE IF NOT EXISTS UsersNoIndex ( id INT UNIQUE, birthDate DATE)```
*Hash index
```CREATE TABLE IF NOT EXISTS UsersHashIndex ( id INT UNIQUE, birthDate DATE,  INDEX birthDate_hash (birthDate) USING HASH```
*Btree index
```CREATE TABLE IF NOT EXISTS UsersBtreeIndex ( id INT UNIQUE, birthDate DATE,  INDEX birthDate_btree (birthDate))```

