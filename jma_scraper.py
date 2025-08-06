import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests


class jma:
    """Simple scraper for JMA AMeDAS data.

    The class downloads hourly AMeDAS observations for a given station
    between two dates and stores the result as CSV. Station information is
    obtained from the AMeDAS_downloader repository.
    """

    STATION_LIST_URL = (
        "https://raw.githubusercontent.com/"
        "KatsuhiroMorishita/AMeDAS_downloader/master/AMeDAS_list.csv"
    )

    def __init__(self) -> None:
        self.stations = self._load_stations()

    def _load_stations(self) -> pd.DataFrame:
        """Load station metadata from the remote CSV file."""
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
        df = pd.read_csv(
            self.STATION_LIST_URL,
            sep="\t",
            names=cols,
            encoding="utf-8",
            engine="python",
        )
        df["name"] = df["name"].str.strip()
        df["block_no"] = df["block_no"].astype(str).str.zfill(4)
        return df

    def amedas(
        self,
        station: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        out_dir: str = "csv",
    ) -> pd.DataFrame:
        """Download hourly AMeDAS data for a given station.

        Parameters
        ----------
        station: str
            Name of the station in Japanese as listed in AMeDAS_list.csv.
        start: datetime, optional
            Start of the interval. Defaults to 24 hours before ``end``.
        end: datetime, optional
            End of the interval. Defaults to ``datetime.utcnow()``.
        out_dir: str
            Base directory where CSV files are stored.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing the scraped data. Empty if download fails.
        """

        end = end or datetime.utcnow()
        start = start or (end - timedelta(days=1))

        row = self.stations[self.stations["name"] == station]
        if row.empty:
            raise ValueError(f"Unknown station: {station}")
        prec_no = row.iloc[0]["prec_no"]
        block_no = row.iloc[0]["block_no"]

        frames = []
        current = start
        while current.date() <= end.date():
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
            current += timedelta(days=1)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)

        # Save to csv/station/...
        station_dir = os.path.join(out_dir, station)
        os.makedirs(station_dir, exist_ok=True)
        fname = f"{station}_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.csv"
        path = os.path.join(station_dir, fname)
        result.to_csv(path, index=False)
        return result


if __name__ == "__main__":
    scraper = jma()
    df = scraper.amedas("東京")
    print(df.head())
