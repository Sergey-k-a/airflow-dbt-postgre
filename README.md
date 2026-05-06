# E-commerce Analytics Pipeline

Аналитический пайплайн для e-commerce данных с использованием Airflow, dbt и PostgreSQL.

raw --> staging --> marts --> analytics


##  Ключевые возможности

###  Оркестрация (Airflow)
- **Умный откат**: автоматическое удаление витрин при ошибках DBT
- **Разделение ресурсов**: DBT-задачи и Python-задачи в разных очередях Celery
- **Валидация данных**: проверка источников до запуска трансформаций
- **Детальное логирование** каждого шага пайплайна

###  Трансформации (dbt)
- **Многослойная архитектура**: Raw → Staging → Marts → Analytics
- **SCD Type 2**: отслеживание изменений клиентов через снапшоты
- **RFM-сегментация**: разделение клиентов по Recency, Frequency, Monetary
- **Когортный анализ**: удержание клиентов по месяцам регистрации
- **Инкрементальные модели**: эффективная загрузка новых данных

###  Аналитика
- **Daily Sales Summary**: ежедневная сводка продаж с маржинальностью
- **Customer 360**: витрина клиентов с сегментацией и активностью
- **Качество данных**: 50+ тестов на уникальность, целостность и бизнес-правила

##  Стек технологий

| Технология            | Назначение                        |
|-----------------------|-----------------------------------|
| **Apache Airflow**    | Оркестрация пайплайна             |
| **dbt**               | Трансформация данных              |
| **PostgreSQL**        | Хранилище данных                  |
| **Docker**            | Контейнеризация                   |
| **Celery**            | Распределённое выполнение задач   | 
| **Redis**             | Брокер сообщений для Celery       |

## Структура проекта

    ├── dags/
    │   └── ecommerce_pipeline.py
    ├── dbt/
    │   ├── models/
    │   │   ├── staging/
    │   │   ├── marts/
    │   │   └── analytics/
    │   ├── snapshots/
    │   ├── seeds/
    │   └── tests/
    ├── scripts/
    ├── docker-compose.yml
    └── Dockerfile

## Быстрый старт

### Предварительные требования
- Docker и Docker Compose
- Git

### Установка и запуск


# Клонируйте репозиторий
git clone https://github.com/Sergey-k-a/airflow-dbt-postgre.git
cd ecommerce-analytics-pipeline

### Создайте .env файл

```bash
cat > .env << EOF
POSTGRES_USER=dbt
POSTGRES_PASSWORD=dbt
POSTGRES_DB=dbt
POSTGRES_PORT=5466
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://dbt:dbt@postgres-dbt:5432/dbt
AIRFLOW__WEBSERVER__SECRET_KEY=mysecretkey
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=INFO
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
EOF

## 🎲 Генерация тестовых данных

Проект включает скрипт для генерации реалистичных данных.
# Зайти в контейнер airflow-worker-dbt
docker compose exec -it airflow-worker-dbt bash
# Запустить генерацию
cd /opt/airflow/scripts
python3 generate_data.py

# Запустите сервисы
docker-compose up -d

# Доступ к сервисам:
Airflow UI: http://localhost:8054 (admin/admin)
dbt Docs:   http://localhost:8066



## 🎲 Генерация тестовых данных

Проект включает скрипт для генерации реалистичных данных.
# Зайти в контейнер airflow-worker
docker compose exec -it airflow-worker-dbt bash
# Запустить генерацию
cd /opt/airflow/scripts
python3 generate_data.py
```


##  Модели данных

# Staging — очистка и стандартизация

stg_customers: стандартизация стран, сегментация клиентов по tier

stg_orders: категоризация статусов, расчёт скидок, временные метрики

stg_products: классификация по цене, категории товаров, флаги новинок

# Marts — бизнес-логика

dim_customers: RFM-сегментация (Champions, Loyal, At Risk и др.)

fct_daily_sales: метрики продаж с маржинальностью по дням

fct_orders_incremental: инкрементальная загрузка заказов с историей

# Analytics — отчётность

daily_sales_summary: агрегация по дням, странам, категориям с rolling averages

customer_cohorts: когортный анализ удержания клиентов по месяцам

## Проект включает 60+ автоматических тестов

## Мониторинг и отказоустойчивость

Автоматические ретраи: повторные попытки при временных сбоях

Smart rollback: выборочный откат только DBT-схем при ошибках трансформации

Pre-flight валидация: проверка источников перед запуском пайплайна

Изоляция очередей: ресурсоёмкие задачи в отдельных Celery worker'ах

Документирование: автогенерация dbt docs с lineage графом

## Итог

Полный ETL/ELT пайплайн от сырых данных до аналитических отчётов

SCD Type 2 для отслеживания исторических изменений клиентов

RFM-сегментация клиентов (7 сегментов + оценка риска оттока)

Когортный анализ удержания по месяцам

Инкрементальная загрузка данных с дедупликацией

60+ автоматических тестов качества данных

Умная обработка ошибок с выборочным откатом

Интерактивная документация данных через dbt docs