# OBJETIVO

Criar uma biblioteca open‑source em Go que ofereça uma experiência semelhante ao Pandas, mas usando o DuckDB como motor de execução.

## MOTIVAÇÃO

O DuckDB já provou ser um “SQLite para analytics”: rápido, embutido, vetorizado, com suporte a colunas, e extremamente eficiente para workloads analíticos. Ele resolve um dos maiores desafios de quem tenta criar um dataframe do zero: um motor de execução otimizado.
Ao usar o DuckDB como backend podemos:

* Herdar toda a performance do engine vetorizado.
* Ganhar paralelismo automático.
* Evitar reinventar parsing, otimização de queries, operadores, etc.
* Oferecer ao usuário uma API simples, enquanto o DuckDB faz o trabalho pesado.

O Pandas não tinha no início — e por isso hoje existe Polars, que usa Apache Arrow e Rust para resolver limitações de performance. A minha proposta é algo nessa linha, mas para Go.

## TRANSFORMAÇÃO

Quero poder transformar o uso de Go em Data Engineering / Data Science
Go é amado por:

* Simplicidade
* Performance
* Concurrency
* Deploy fácil
* Binários estáticos
* Ecossistema maduro para backend e infra

Mas, eu percebi que Go ainda não tem uma biblioteca de dataframes realmente dominante. Isso cria uma barreira enorme para cientistas de dados e engenheiros que gostariam de usar Go além de pipelines e serviços.

Então, a minha proposta é entregar:

* Uma API simples
* Uma experiência parecida com Pandas
* Performance de DuckDB
* Integração com Arrow, Parquet, CSV, JSON, SQL

E com isso, eu quero abrir a porta para que Go seja usado em:

* ETL/ELT
* Feature engineering
* Prototipação de modelos
* Data exploration
* Machine learning pipelines
* Data apps embarcados

Acredito que isso será um divisor de águas.

## SUGESTÕES PRÁTICAS

Sugestões práticas para o design da biblioteca:

*1.* Uma API familiar, inspirada no Pandas/Polars, algo como:

```go
df := goframe.ReadCSV("data.csv")

df = df.Filter("age > 30").
        GroupBy("country").
        Agg("salary", "mean")

df.Show()
````

Ou até:

```go
df.Sql("SELECT country, AVG(salary) FROM df GROUP BY country")
```

A familiaridade reduz a curva de aprendizado.

*2.* Internamente, tudo vira DuckDB, o dataframe pode ser:

* Uma tabela temporária no DuckDB
* Um view
* Um arquivo Arrow/Parquet mapeado

Isso dará flexibilidade e performance.

*3.* Suporte nativo a formatos modernos:

* CSV
* Parquet
* Arrow IPC
* JSON Lines
* SQLite
* Postgres/MySQL connectors (opcional)

*4.* Integração com Go routines, será um diferencial que Pandas não tem:

* Operações paralelas
* Pipelines concorrentes
* Streaming de dados
Go brilha aqui.

*5.* Uma camada de abstração que esconda SQL quando o usuário quiser, mas permita SQL quando ele quiser também.

*6.* Documentação impecável e exemplos reais, isso é o que fez o Pandas explodir.

*7.* Benchmarks contra Pandas e Polars, nada atrai mais atenção do que:

* “10x mais rápido que Pandas”
* “Tão rápido quanto Polars, mas em Go”
* “Integração nativa com serviços Go”

## MVP

Primeiro MVP:

* ReadCSV
* Select
* Filter
* GroupBy
* Agg
* Show

Depois expandir para outras funcionalidades.

## NOME DA BIBLIOTECA

* duckframe (alias, df)

## COMUNIDADE

Será construída uma comunidade desde o início:

* Discord/Slack
* Roadmap público
* Issues bem organizadas
* Exemplos reais
* Benchmarks

## IMPORTANTE

Fazer integração com o ecossistema Go:

* go generate
* go test
* go vet
* go doc
* go mod tidy
