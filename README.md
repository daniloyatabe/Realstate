# Realstate Project

Ferramenta para coletar diariamente informações de aluguel por m² em bairros de São Paulo utilizando o [Zap Imóveis](https://www.zapimoveis.com.br/).

## Funcionalidades

* Coleta automática dos preços de apartamentos para aluguel nos bairros **Pinheiros**, **Vila Madalena**, **Moema** e **Saúde**.
* Diferencia apartamentos mobiliados e não mobiliados e calcula o preço por metro quadrado de cada anúncio.
* Armazena o histórico de preços em um banco de dados SQLite para facilitar análises futuras.
* Gera gráficos com a variação de preço de um mesmo imóvel e com o preço médio por m² de cada bairro.

## Pré-requisitos

* Python 3.11 ou superior.
* Dependências opcionais:
  * `matplotlib` para geração de gráficos (`pip install matplotlib`).

## Estrutura do projeto

```
realstate/
├── config.py            # Configurações e constantes principais
├── scraper.py           # Cliente HTTP e normalização dos anúncios
├── storage.py           # Persistência em SQLite
├── reporting.py         # Funções para criação de gráficos
└── main.py              # Interface de linha de comando
tests/                   # Testes unitários (executáveis com `python -m unittest`)
fixtures/                # Dados de exemplo usados nos testes
data/                    # Pasta sugerida para o banco de dados SQLite e gráficos
```

## Executando a coleta

Para executar uma coleta única e armazenar os resultados em `data/zap_rentals.sqlite`:

```bash
python -m realstate.main collect
```

Por padrão são coletadas informações dos bairros Pinheiros, Vila Madalena, Moema e Saúde. Para limitar a coleta a bairros específicos utilize a opção `--neighborhood` múltiplas vezes:

```bash
python -m realstate.main collect --neighborhood "Pinheiros" --neighborhood "Moema"
```

### Execução diária automática

Utilize a flag `--daily` para manter a coleta ativa e repetir o processo a cada 24 horas (ou outro intervalo definido por `--interval-hours`):

```bash
python -m realstate.main collect --daily --interval-hours 24
```

Este comando pode ser agendado via `cron` (Linux/macOS) ou Agendador de Tarefas (Windows) para iniciar automaticamente junto com o sistema operacional.

## Gerando gráficos

Após coletar dados, é possível gerar gráficos diretamente pela linha de comando:

* **Histórico de preço de um imóvel específico**:

  ```bash
  python -m realstate.main report listing ZAP_ID --output graficos/apartamento.png
  ```

  Substitua `ZAP_ID` pelo código do imóvel (ex.: `123456`). Utilize `--show` para exibir o gráfico em vez de salvá-lo em arquivo.

* **Preço médio por m² de um bairro**:

  ```bash
  python -m realstate.main report neighborhood "Pinheiros" --output graficos/pinheiros.png
  ```

Os gráficos distinguem automaticamente imóveis mobiliados e não mobiliados.

## Testes

Para validar as funcionalidades principais execute:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

Os testes utilizam dados artificiais que simulam a resposta da API do Zap Imóveis para garantir o funcionamento das rotinas de coleta e armazenamento.
