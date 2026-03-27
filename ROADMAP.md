# DuckFrame — Roadmap

> Status: 🟡 Em andamento | Início: Março 2026

---

## Fase 0 — Setup do Projeto
- [x] Inicializar módulo Go (`go mod init`)
- [x] Definir estrutura de diretórios do projeto
- [x] Adicionar dependência do DuckDB (driver Go)
- [x] Configurar CI básico (GitHub Actions: build + test)
- [x] Criar `.gitignore`, `LICENSE`, `Makefile`
- [x] Validar compilação e conexão básica com DuckDB in-memory

## Fase 1 — Core: Structs e Conexão
- [x] Definir struct `DataFrame` (referência à tabela/view no DuckDB)
- [x] Criar gerenciamento de conexão DuckDB (pool, lifecycle)
- [x] Implementar criação de tabelas temporárias a partir de dados
- [x] Implementar `Close()` / cleanup de recursos
- [x] Testes unitários para conexão e lifecycle

## Fase 2 — MVP: Operações Básicas
- [x] `ReadCSV(path string) *DataFrame` — leitura de CSV via DuckDB
- [x] `Select(cols ...string) *DataFrame` — seleção de colunas
- [x] `Filter(expr string) *DataFrame` — filtragem com expressão
- [x] `GroupBy(cols ...string) *GroupedFrame` — agrupamento
- [x] `Agg(col string, fn string) *DataFrame` — agregação (mean, sum, count, min, max)
- [x] `Show()` — exibição formatada no terminal (tabela)
- [x] `Sql(query string) *DataFrame` — execução de SQL direto
- [x] Testes unitários para cada operação do MVP
- [x] Exemplo funcional end-to-end com CSV real

## Fase 3 — API Fluente e Encadeamento
- [x] Garantir encadeamento de operações (`df.Filter(...).Select(...).Show()`)
- [x] Implementar propagação de erros em cadeia (error-safe chaining)
- [x] `Collect() []map[string]interface{}` — materializar resultado em Go
- [x] `ToSlice(dest interface{})` — materializar em slice de structs
- [x] Tratamento de erros consistente (error wrapping + `Err()`)

## Fase 4 — Formatos de Dados
- [x] `ReadParquet(path string) *DataFrame`
- [x] `ReadJSON(path string) *DataFrame` (JSON Lines)
- [ ] `ReadArrow(path string) *DataFrame` (Arrow IPC) — adiado
- [x] `WriteCSV(path string) error`
- [x] `WriteParquet(path string) error`
- [x] `WriteJSON(path string) error`
- [x] Testes para leitura/escrita de cada formato

## Fase 5 — Operações Avançadas
- [x] `Sort(col string, asc bool) (*DataFrame, error)`
- [x] `Limit(n int) (*DataFrame, error)`
- [x] `Distinct() (*DataFrame, error)`
- [x] `Rename(old, new string) (*DataFrame, error)`
- [x] `Drop(cols ...string) (*DataFrame, error)`
- [x] `WithColumn(name string, expr string) (*DataFrame, error)`
- [x] `Join(other *DataFrame, on string, how string) (*DataFrame, error)` — inner, left, right, full
- [x] `Union(other *DataFrame) (*DataFrame, error)`
- [x] `Head(n int) (*DataFrame, error)` / `Tail(n int) (*DataFrame, error)`
- [x] `Shape() (rows int, cols int)` — já existia (Fase 1)
- [x] `Columns() []string` — já existia (Fase 1)
- [x] `Dtypes() (map[string]string, error)`
- [x] `Describe() (*DataFrame, error)` — estatísticas descritivas (count, mean, std, min, max)

## Fase 6 — Concorrência e Streaming
- [x] Pipeline concorrente: `ParallelApply` — processar múltiplos DataFrames em paralelo
- [x] Streaming de leitura: `ReadCSVChunked` — leitura chunked com canal
- [x] Context support: `FromQueryContext`, `ReadCSVContext`, `FilterContext`, `SortContext`
- [x] Benchmarks: `duckframe_bench_test.go` — Sequential vs Parallel (Filter, Sort+Limit, Chunked)
- [x] Fix: `SetMaxOpenConns(1)` — tabelas temporárias são connection-scoped no DuckDB

## Fase 7 — Conectores Externos
- [ ] `ReadSQLite(path, table string) *DataFrame`
- [ ] `ReadPostgres(dsn, query string) *DataFrame` (opcional)
- [ ] `ReadMySQL(dsn, query string) *DataFrame` (opcional)
- [ ] `ReadFromDB(db *sql.DB, query string) *DataFrame` — genérico via database/sql

## Fase 8 — Qualidade e Ecossistema Go
- [ ] Cobertura de testes ≥ 80%
- [ ] `go vet` sem warnings
- [ ] `golangci-lint` configurado e limpo
- [ ] `go doc` — documentação exportada em todas as funções públicas
- [ ] `go generate` para código gerado (se aplicável)
- [ ] Exemplos em `_example_test.go`
- [ ] Godoc publicado

## Fase 9 — Documentação e Exemplos
- [ ] README.md com badges, instalação, quickstart
- [ ] Pasta `examples/` com casos de uso reais
  - [ ] ETL básico (CSV → Parquet)
  - [ ] Análise exploratória
  - [ ] Pipeline concorrente
  - [ ] Integração com API HTTP
- [ ] Documentação de API (wiki ou site estático)
- [ ] CONTRIBUTING.md

## Fase 10 — Benchmarks e Lançamento
- [ ] Benchmark: DuckFrame vs Pandas (Python) em operações comuns
- [ ] Benchmark: DuckFrame vs Polars (Rust/Python) em operações comuns
- [ ] Benchmark: DuckFrame vs Gota (Go) em operações comuns
- [ ] Publicar resultados no README
- [ ] Release v0.1.0 — MVP público
- [ ] Post de lançamento (blog / Reddit / Hacker News / Twitter)
- [ ] Criar Discord/Slack da comunidade

---

## Legenda

| Símbolo | Status |
|---------|--------|
| `[ ]`   | Pendente |
| `[~]`   | Em andamento |
| `[x]`   | Concluído |
