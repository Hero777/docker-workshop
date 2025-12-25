#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

# Словарь типов данных: принудительно задаем типы для экономии памяти 
# и во избежание ошибок интерпретации (особенно важно для Int64, который поддерживает NULL)
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# Колонки, которые Pandas должен сразу преобразовать в формат даты/времени
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    # Создаем итератор для чтения файла. 
    # iterator=True и chunksize позволяют не загружать файл весом в гигабайты целиком в RAM.
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    # Забираем первую порцию (чанк) данных
    first_chunk = next(df_iter)

    # Трюк: берем только заголовки (head(0)) и создаем пустую таблицу в Postgres.
    # if_exists="replace" удалит старую таблицу, если она была.
    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print(f"Table {target_table} created")

    # Записываем первую порцию данных в уже созданную пустую таблицу
    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append" # Важно: здесь уже "append", чтобы не перезатереть структуру
    )

    print(f"Inserted first chunk: {len(first_chunk)}")

    # Основной цикл: перебираем оставшиеся чанки из итератора
    # tqdm отрисовывает прогресс-бар в консоли
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)}")

    print(f'done ingesting to {target_table}')

@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-pass', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default='5432', show_default=True, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
@click.option('--year', default=2021, show_default=True, type=int, help='Year of data')
@click.option('--month', default=1, show_default=True, type=int, help='Month of data (1-12)')
@click.option('--chunksize', default=100000, show_default=True, type=int, help='CSV chunksize')
@click.option('--target-table', 'target_table', default='yellow_taxi_data', show_default=True, help='Target DB table')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):

    # Создаем движок SQLAlchemy для работы с Postgres
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Формируем URL для скачивания конкретного файла (архив .csv.gz)
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    # Запуск процесса загрузки
    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()