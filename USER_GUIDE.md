# DuckFrame — Guia do Usuário

> Biblioteca de DataFrames para Go, powered by DuckDB.

---

## Índice

1. [Pré-requisitos](#pré-requisitos)
2. [Instalação](#instalação)
3. [Estrutura do Projeto](#estrutura-do-projeto)
4. [Uso Básico](#uso-básico)
5. [Desenvolvimento](#desenvolvimento)
6. [API Reference](#api-reference)

---

## Pré-requisitos

- **Go** >= 1.22
- **CGO habilitado** (`CGO_ENABLED=1`) — necessário para o driver DuckDB
- **Compilador C** instalado (gcc/clang)
  - macOS: `xcode-select --install`
  - Ubuntu/Debian: `sudo apt install build-essential`

## Instalação

```bash
go get github.com/lserra/duckframe
```

## Estrutura do Projeto

```
duckframe/
├── duckframe.go              # Struct DataFrame + API principal
├── duckframe_test.go          # Testes do DataFrame
├── internal/
│   └── engine/
│       ├── engine.go          # Gerenciamento de conexão DuckDB
│       └── engine_test.go     # Testes da engine
├── examples/                  # Exemplos de uso
├── testdata/                  # Dados para testes
├── Makefile                   # Comandos de build/test/lint
├── .github/workflows/ci.yml  # CI com GitHub Actions
├── LICENSE                    # MIT
├── ROADMAP.md                 # Roadmap do projeto
└── USER_GUIDE.md              # Este guia
```

## Uso Básico

### Conectando ao DuckDB (in-memory)

```go
package main

import (
    "fmt"
    "log"

    "github.com/lserra/duckframe/internal/engine"
)

func main() {
    // Abrir conexão in-memory (string vazia)
    db, err := engine.Open("")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Verificar versão do DuckDB
    var version string
    db.Conn().QueryRow("SELECT version()").Scan(&version)
    fmt.Println("DuckDB version:", version)
}
```

### Conectando com arquivo persistente

```go
db, err := engine.Open("meus_dados.duckdb")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### Executando queries SQL diretamente

```go
db, err := engine.Open("")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

conn := db.Conn()

// Criar tabela
conn.Exec("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")

// Inserir dados
conn.Exec("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

// Consultar
rows, _ := conn.Query("SELECT name, age FROM users WHERE age > 28")
defer rows.Close()

for rows.Next() {
    var name string
    var age int
    rows.Scan(&name, &age)
    fmt.Printf("%s (%d anos)\n", name, age)
}
```

## Desenvolvimento

### Comandos disponíveis (Makefile)

```bash
make build      # Compilar o projeto
make test       # Rodar todos os testes
make coverage   # Gerar relatório de cobertura
make fmt        # Formatar código
make vet        # Análise estática
make lint       # Linter (requer golangci-lint)
make tidy       # Limpar dependências
make clean      # Limpar artefatos
make all        # fmt + vet + lint + test + build
```

### Rodando testes manualmente

```bash
CGO_ENABLED=1 go test ./... -v
```

## API Reference

### Engine

#### `engine.Open(path string) (*DB, error)`

Abre uma conexão com o DuckDB.

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `path` | `string` | Caminho do arquivo `.duckdb`. Use `""` para in-memory. |

**Retorna:** `*DB` (wrapper da conexão) e `error`.

#### `(*DB).Conn() *sql.DB`

Retorna a conexão `*sql.DB` subjacente para executar queries diretamente.

#### `(*DB).Close() error`

Fecha a conexão com o DuckDB e libera recursos.

---

### DataFrame

#### `duckframe.New(db *engine.DB, columns []string, rows []map[string]interface{}) (*DataFrame, error)`

Cria um novo DataFrame a partir de colunas e dados. Os tipos são inferidos automaticamente (int→BIGINT, float→DOUBLE, bool→BOOLEAN, default→VARCHAR).

```go
db, _ := engine.Open("")
defer db.Close()

df, err := duckframe.New(db, []string{"name", "age", "salary"}, []map[string]interface{}{
    {"name": "Alice", "age": int64(30), "salary": 85000.0},
    {"name": "Bob",   "age": int64(25), "salary": 72000.0},
})
defer df.Close()
```

#### `duckframe.FromQuery(db *engine.DB, query string) (*DataFrame, error)`

Cria um DataFrame a partir do resultado de uma query SQL.

```go
df, err := duckframe.FromQuery(db, "SELECT * FROM source WHERE score > 9.0")
defer df.Close()
```

#### `(*DataFrame).Columns() []string`

Retorna os nomes das colunas (cópia segura).

#### `(*DataFrame).Shape() (rows int, cols int, err error)`

Retorna o número de linhas e colunas do DataFrame.

```go
r, c, err := df.Shape()
fmt.Printf("Rows: %d, Cols: %d\n", r, c)
```

#### `(*DataFrame).TableName() string`

Retorna o nome da tabela temporária no DuckDB que armazena os dados.

#### `(*DataFrame).Engine() *engine.DB`

Retorna a conexão DuckDB subjacente.

#### `(*DataFrame).Close() error`

Dropa a tabela temporária e libera recursos. Sempre use `defer df.Close()`.

---

### Operações MVP

#### `duckframe.ReadCSV(db *engine.DB, path string) (*DataFrame, error)`

Lê um arquivo CSV e retorna um DataFrame. Usa `read_csv_auto` do DuckDB (detecção automática de tipos e delimitadores).

```go
df, err := duckframe.ReadCSV(db, "data/employees.csv")
defer df.Close()
```

#### `(*DataFrame).Select(cols ...string) (*DataFrame, error)`

Retorna um novo DataFrame com apenas as colunas especificadas.

```go
selected, err := df.Select("name", "salary")
defer selected.Close()
```

#### `(*DataFrame).Filter(expr string) (*DataFrame, error)`

Retorna um novo DataFrame com linhas que satisfazem a expressão SQL.

```go
filtered, err := df.Filter("age > 30")
filtered, err = df.Filter("country = 'Brazil'")
```

#### `(*DataFrame).GroupBy(cols ...string) *GroupedFrame`

Agrupa o DataFrame pelas colunas especificadas. Deve ser seguido de `Agg()`.

#### `(*GroupedFrame).Agg(col string, fn string) (*DataFrame, error)`

Executa uma agregação no grupo. Funções suportadas: `mean`/`avg`, `sum`, `count`, `min`, `max`.

```go
result, err := df.GroupBy("country").Agg("salary", "mean")
defer result.Close()
```

#### `(*DataFrame).Show(maxRows ...int) error`

Exibe o DataFrame formatado no terminal. Por padrão, mostra até 50 linhas.

```go
df.Show()      // até 50 linhas
df.Show(10)    // até 10 linhas
```

Saída:
```
DataFrame [7 rows x 4 cols]
name    age    country    salary
------  -----  ---------  --------
Alice   30     Brazil     85000.5
Bob     25     USA        72000
...
```

#### `(*DataFrame).Sql(query string) (*DataFrame, error)`

Executa SQL direto usando `{df}` como placeholder para a tabela do DataFrame.

```go
result, err := df.Sql("SELECT country, AVG(salary) FROM {df} GROUP BY country")
defer result.Close()
```

---

### Materialização de Dados

#### `(*DataFrame).Collect() ([]map[string]interface{}, error)`

Materializa o DataFrame inteiro em uma slice de maps (cada map é uma linha).

```go
rows, err := df.Collect()
for _, row := range rows {
    fmt.Printf("%s: %.2f\n", row["name"], row["salary"])
}
```

#### `(*DataFrame).ToSlice(dest interface{}) error`

Materializa o DataFrame em uma slice de structs. Os campos são mapeados pela tag `df`, ou pelo nome do campo (case-insensitive).

```go
type Employee struct {
    Name    string  `df:"name"`
    Age     int64   `df:"age"`
    Country string  `df:"country"`
    Salary  float64 `df:"salary"`
}

var employees []Employee
err := df.ToSlice(&employees)
for _, emp := range employees {
    fmt.Printf("%s (%d) - $%.2f\n", emp.Name, emp.Age, emp.Salary)
}
```

### Tratamento de Erros

#### `(*DataFrame).Err() error`

Retorna o erro armazenado no DataFrame (para encadeamento fluente).

Todos os métodos propagam erros automaticamente — se une operação falha, as operações seguintes também retornam erro sem executar:

```go
// Se Filter falha, Select e Collect também retornam o erro
filtered, _ := df.Filter("INVALID")
selected, _ := filtered.Select("name")
rows, err := selected.Collect()  // err contém o erro original do Filter

// Ou verifique com Err()
if filtered.Err() != nil {
    log.Fatal(filtered.Err())
}
```

---

### Formatos de Dados

#### Leitura

##### `duckframe.ReadCSV(db *engine.DB, path string) (*DataFrame, error)`

Lê um arquivo CSV (detecção automática de tipos e delimitadores).

```go
df, err := duckframe.ReadCSV(db, "data/employees.csv")
```

##### `duckframe.ReadParquet(db *engine.DB, path string) (*DataFrame, error)`

Lê um arquivo Parquet.

```go
df, err := duckframe.ReadParquet(db, "data/employees.parquet")
```

##### `duckframe.ReadJSON(db *engine.DB, path string) (*DataFrame, error)`

Lê um arquivo JSON Lines (newline-delimited JSON).

```go
df, err := duckframe.ReadJSON(db, "data/employees.jsonl")
```

#### Escrita

##### `(*DataFrame).WriteCSV(path string) error`

Exporta o DataFrame para CSV.

```go
err := df.WriteCSV("output/result.csv")
```

##### `(*DataFrame).WriteParquet(path string) error`

Exporta o DataFrame para Parquet.

```go
err := df.WriteParquet("output/result.parquet")
```

##### `(*DataFrame).WriteJSON(path string) error`

Exporta o DataFrame para JSON.

```go
err := df.WriteJSON("output/result.json")
```

#### Pipeline ETL: CSV → Parquet

```go
db, _ := engine.Open("")
defer db.Close()

// Ler CSV, filtrar, salvar como Parquet
df, _ := duckframe.ReadCSV(db, "raw_data.csv")
defer df.Close()

filtered, _ := df.Filter("salary > 80000")
defer filtered.Close()

filtered.WriteParquet("high_salary.parquet")
```

---

> **Nota:** Este guia será expandido à medida que novas funcionalidades forem implementadas.