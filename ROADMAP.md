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
- [ ] `ReadParquet(path string) *DataFrame`
- [ ] `ReadJSON(path string) *DataFrame` (JSON Lines)
- [ ] `ReadArrow(path string) *DataFrame` (Arrow IPC)
- [ ] `WriteCSV(path string) error`
- [ ] `WriteParquet(path string) error`
- [ ] `WriteJSON(path string) error`
- [ ] Testes para leitura/escrita de cada formato

## Fase 5 — Operações Avançadas
- [ ] `Sort(col string, asc bool) *DataFrame`
- [ ] `Limit(n int) *DataFrame`
- [ ] `Distinct() *DataFrame`
- [ ] `Rename(old, new string) *DataFrame`
- [ ] `Drop(cols ...string) *DataFrame`
- [ ] `WithColumn(name string, expr string) *DataFrame`
- [ ] `Join(other *DataFrame, on string, how string) *DataFrame`
- [ ] `Union(other *DataFrame) *DataFrame`
- [ ] `Head(n int) *DataFrame` / `Tail(n int) *DataFrame`
- [ ] `Shape() (rows int, cols int)`
- [ ] `Columns() []string`
- [ ] `Dtypes() map[string]string`
- [ ] `Describe() *DataFrame` — estatísticas descritivas

## Fase 6 — Concorrência e Streaming
- [ ] Pipeline concorrente: processar múltiplos DataFrames em paralelo
- [ ] Streaming de leitura para arquivos grandes (chunked reading)
- [ ] Context support (`context.Context`) para cancelamento e timeout
- [ ] Benchmarks de operações concorrentes vs sequenciais

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
