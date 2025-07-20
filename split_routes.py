from sqlalchemy import create_engine
from gtfs_functions import Feed


def load_gtfs_feed(file_path, start_date, end_date):
    feed = Feed(file_path, start_date=start_date, end_date=end_date)
    segments_gdf = feed.segments
    return segments_gdf


def save_to_postgis(gdf, table_name, db_host, db_port, db_user, db_pass, db_name):
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_url)
    gdf.to_postgis(table_name, engine, if_exists="replace")
    print(f"Data saved to {table_name} table in the {db_name} database")


def main() -> None:
    gtfs_file = "../hsl.zip"
    gtfs_segments = load_gtfs_feed(gtfs_file, "2025-07-09", "2025-09-06")
    save_to_postgis(
        gtfs_segments,
        "segments",
        "localhost",
        "5434",
        "postgres",
        "mobility",
        "urbanmobility",
    )


if __name__ == "__main__":
    main()
