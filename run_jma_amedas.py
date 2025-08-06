from jma_scraper import jma


def main() -> None:
    scraper = jma()
    cities = [
        "札幌",
        "仙台",
        "東京",
        "名古屋",
        "金沢",
        "大阪",
        "広島",
        "高松(香川県)",
        "福岡",
    ]

    for city in cities:
        scraper.amedas(city, granularity="hourly")
        print(f"Fetching AMeDAS for {city}")
    print()


if __name__ == "__main__":
    main()
