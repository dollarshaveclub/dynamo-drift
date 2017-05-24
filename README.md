# dynamo-drift
[![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)][godocs]

[godocs]: https://godoc.org/github.com/dollarshaveclub/dynamo-drift


dynamo-drift is a minimalistic library for performing DynamoDB schema migrations.

Individual migrations are code that is executed in the context of a callback performed per existing table item.

It is the responsibility of the application to determine:
- what a migration consists of and when it may be executed
- how many migrations exist at any given time

dynamo-drift:
- keeps track of migrations already executed (via a separate metadata table)
- executes the supplied migrations
- queues and executes actions (insert/update/delete) after callbacks finish so that they do not interfere with the main table scan

Example
-------

```go
import (
  "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
  "github.com/dollarshaveclub/dynamo-drift"
)

func main() {
  // get authenticated DynamoDB client
  client := dynamodb.New(session.Must(session.NewSession()))

  // create migrator
  dd := drift.DynamoDrifter{}
  dd.MetaTableName = "MyMigrationsTable"
  dd.DynamoDB = client

  // initialize
  dd.Init(10, 10)

  // check applied
  migrations, _ := dd.Applied()
  fmt.Printf("migrations already applied: %v\n", len(migrations)) // "migrations already applied: 0"

  // create migration
  migration := &drift.DynamoDrifterMigration{
    Number: 0,
    TableName: "MyApplicationTable",
    Description: "readme test",
    Callback: migrateUp,
  }

  // run migration
  errs := dd.Run(context.TODO(), migration, 1, true)
  for _, err := range errs {
    fmt.Printf("error during up migration: %v", err)
  }

  migrations, _ = dd.Applied()
  fmt.Printf("migrations already applied: %v\n", len(migrations)) // "migrations already applied: 1"

  migration.Callback = migrateDown

  // run undo migration
  errs := dd.Undo(context.TODO(), migration, 1, true)
  for _, err := range errs {
    fmt.Printf("error during down migration: %v", err)
  }

  migrations, _ = dd.Applied()
  fmt.Printf("migrations already applied: %v\n", len(migrations)) // "migrations already applied: 0"
}

type MyDynamoKey struct {
  ID int `dynamodb:"ID"`
}

type MyOldDynamoItem struct {
  Name string `dynamodb:":n"`
}

type MyNewDynamoItem struct {
  FirstName string `dynamodb:":fn"`
  LastName string `dynamodb:":ln"`
}

// Callbacks are executed once for each item in the target table
func migrateUp(item drift.RawDynamoItem, action *drift.DrifterAction) {
  name := item["Name"]
  ns := strings.Split(name, " ")
  newitem := MyNewDynamoItem{
    FirstName: ns[0],
    LastName: ns[1],
  }
  key := MyDynamoKey{
    ID: item["ID"],
  }
  action.Update(key, newitem, "SET FirstName = :fn, LastName = :ln", nil, "")
}

func migrateDown(item drift.RawDynamoItem, action *drift.DrifterAction) {
  olditem := MyOldDynamoItem{
    Name: item["FirstName"] + item["LastName"],
  }
  key := MyDynamoKey{
    ID: item["ID"],
  }
  action.Update(key, olditem, "SET Name = :n REMOVE FirstName, LastName", nil, "")
}
```
