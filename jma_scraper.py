import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests


# Explicitly name the timezone so log output shows "JST"
JST = timezone(timedelta(hours=9), "JST")


class jma:
    """Simple scraper for JMA AMeDAS data.

    The class downloads AMeDAS observations for a given station between
    two dates and stores the result as CSV. Station information is
    obtained from the AMeDAS_downloader repository.
    """

    STATION_LIST_URL = (
        "https://raw.githubusercontent.com/"
        "KatsuhiroMorishita/AMeDAS_downloader/master/AMeDAS_list.csv"
    )
    STATION_LIST_LOCAL = os.path.join(
        os.path.dirname(__file__), "AMeDAS_list.csv"
    )

    def __init__(self) -> None:
        self.stations = self._load_stations()

    def _load_stations(self) -> pd.DataFrame:
        """Load station metadata, updating a local cached CSV file."""
        cols = [
            "prec_no",
            "block_no",
            "name",
            "group_name",
            "degree_lat",
            "degree_lon",
            "height",
            "station_id",
            "area_code",
            "group_code",
        ]
        try:
            response = requests.get(self.STATION_LIST_URL, timeout=10)
            response.raise_for_status()
            with open(self.STATION_LIST_LOCAL, "wb") as fh:
                fh.write(response.content)
        except Exception as exc:
            print(f"Failed to update station list: {exc}")
            if not os.path.exists(self.STATION_LIST_LOCAL):
                raise

        df = pd.read_csv(
            self.STATION_LIST_LOCAL,
            sep="\t",
            names=cols,
            encoding="utf-8",
            engine="python",
        )
        df["name"] = df["name"].str.strip()
        df["group_name"] = df["group_name"].str.strip()
        df["block_no"] = df["block_no"].astype(str).str.zfill(4)
        return df

    def amedas(
        self,
        station: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        granularity: str = "all",
        out_dir: str = "csv/amedas",
    ) -> pd.DataFrame:
        """Download AMeDAS data for a given station.

        Parameters
        ----------
        station: str
            Name of the station in Japanese as listed in AMeDAS_list.csv. If
            multiple stations share the same name, specify the prefecture or
            region in parentheses, e.g. ``"高松(香川県)"``.
        start: datetime, optional
            Start of the interval. Defaults to 24 hours before ``end``.
        end: datetime, optional
            End of the interval. Defaults to 1 day before the current time in
            Japan (``datetime.now(JST) - timedelta(days=1)``).
        granularity: str
            Time granularity of the data. ``"hourly"`` downloads data for each
            day between ``start`` and ``end`` and stores them under
            ``{out_dir}/YYYY/MM/station_YYYYMMDD.csv``. ``"daily"`` downloads
            one CSV per month to ``{out_dir}/YYYY/station_YYYYMM.csv`` and
            ``"monthly"`` downloads yearly summaries to
            ``{out_dir}/station_YYYY.csv``. ``"all"`` downloads all three
            granularities and concatenates the results. This is the default.
        out_dir: str
            Base directory where CSV files are stored.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing the scraped data. Empty if download fails.
        """

        if end:
            end = (
                end.astimezone(JST) if end.tzinfo else end.replace(tzinfo=JST)
            ).date()
        else:
            end = (datetime.now(JST) - timedelta(days=1)).date()

        if start:
            start = (
                start.astimezone(JST)
                if start.tzinfo
                else start.replace(tzinfo=JST)
            ).date()
        else:
            start = end - timedelta(days=0)

        print(f"Fetching AMeDAS for {station} from {start} to {end} (JST)")

        if granularity == "all":
            dfs = []
            for g in ("hourly", "daily", "monthly"):
                df = self.amedas(station, start, end, g, out_dir)
                if not df.empty:
                    dfs.append(df)
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        station_name = station
        group_name = None
        if "(" in station and station.endswith(")"):
            station_name, group_name = station[:-1].split("(", 1)
            station_name = station_name.strip()
            group_name = group_name.strip()

        if group_name:
            row = self.stations[
                (self.stations["name"] == station_name)
                & (self.stations["group_name"] == group_name)
            ]
        else:
            row = self.stations[self.stations["name"] == station_name]
            if len(row) > 1:
                raise ValueError(
                    "Station name is not unique. Specify as 'name(prefecture)'."
                )

        if row.empty:
            raise ValueError(f"Unknown station: {station}")
        prec_no = row.iloc[0]["prec_no"]
        block_no = row.iloc[0]["block_no"]

        frames = []

        if granularity == "hourly":
            current = start
            while current <= end:
                url = (
                    "https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?"
                    f"prec_no={prec_no}&block_no={block_no}&year={current.year}"
                    f"&month={current.month}&day={current.day}&view="
                )
                try:
                    tables = pd.read_html(
                        url,
                        encoding="utf-8",
                        header=0,
                    )
                except Exception as exc:  # network error or parser error
                    print(f"Failed to fetch {url}: {exc}")
                    current += timedelta(days=1)
                    continue

                if not tables:
                    current += timedelta(days=1)
                    continue

                df = tables[0].dropna(how="all")
                df.insert(0, "date", current.strftime("%Y-%m-%d"))
                frames.append(df)

                # Save to csv/YYYY/MM/station_YYYYMMDD.csv
                year = current.strftime("%Y")
                month = current.strftime("%m")
                day = current.strftime("%Y%m%d")
                day_dir = os.path.join(out_dir, year, month)
                os.makedirs(day_dir, exist_ok=True)
                fname = f"{station}_{day}.csv"
                df.to_csv(os.path.join(day_dir, fname), index=False)

                current += timedelta(days=1)

        elif granularity == "daily":
            start_month = datetime(start.year, start.month, 1, tzinfo=JST)
            end_month = datetime(end.year, end.month, 1, tzinfo=JST)
            months = pd.date_range(start=start_month, end=end_month, freq="MS")
            for current in months:
                url = (
                    "https://www.data.jma.go.jp/stats/etrn/view/daily_s1.php?"
                    f"prec_no={prec_no}&block_no={block_no}&year={current.year}"
                    f"&month={current.month:02d}&day=01&view=p1"
                )
                try:
                    tables = pd.read_html(
                        url,
                        encoding="utf-8",
                        header=0,
                    )
                except Exception as exc:
                    print(f"Failed to fetch {url}: {exc}")
                    continue

                if not tables:
                    continue

                df = tables[0].dropna(how="all")
                df.rename(columns={df.columns[0]: "day"}, inplace=True)
                df = df[pd.to_numeric(df["day"], errors="coerce").notna()]
                df["date"] = pd.to_datetime(
                    {
                        "year": current.year,
                        "month": current.month,
                        "day": df["day"].astype(int),
                    }
                ).dt.strftime("%Y-%m-%d")
                frames.append(df)

                year_dir = os.path.join(out_dir, f"{current.year:04d}")
                os.makedirs(year_dir, exist_ok=True)
                fname = f"{station}_{current.strftime('%Y%m')}.csv"
                df.to_csv(os.path.join(year_dir, fname), index=False)

        elif granularity == "monthly":
            start_year = datetime(start.year, 1, 1, tzinfo=JST)
            end_year = datetime(end.year, 1, 1, tzinfo=JST)
            years = pd.date_range(start=start_year, end=end_year, freq="YS")
            for current in years:
                url = (
                    "https://www.data.jma.go.jp/stats/etrn/view/monthly_s1.php?"
                    f"prec_no={prec_no}&block_no={block_no}&year={current.year}"
                    "&month=01&day=01&view=p1"
                )
                try:
                    tables = pd.read_html(
                        url,
                        encoding="utf-8",
                        header=0,
                    )
                except Exception as exc:
                    print(f"Failed to fetch {url}: {exc}")
                    continue

                if not tables:
                    continue

                df = tables[0].dropna(how="all")
                frames.append(df)

                os.makedirs(out_dir, exist_ok=True)
                fname = f"{station}_{current.strftime('%Y')}.csv"
                df.to_csv(os.path.join(out_dir, fname), index=False)

        else:
            raise ValueError("Unsupported granularity")

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        return result


if __name__ == "__main__":
    scraper = jma()
    cities = ["札幌", "仙台", "東京", "名古屋", "金沢", "大阪", "広島", "高松(香川県)", "福岡"]
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2025, 8, 5)

    for city in cities:
        scraper.amedas(city,start_date,end_date,"hourly")
        print(f"Fetching AMeDAS for {city}: {start_date} to {end_date}")
    print()
